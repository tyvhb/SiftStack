"""Enrich notices with property data from the OpenWeb Ninja Real-Time Zillow Data API.

Processes a list of NoticeData records one at a time, populating property detail
fields (valuation, equity, MLS status, bedrooms, etc.) with data from Zillow.

Graceful degradation: if no API key or API errors, all notices pass through
unchanged.
"""

import logging
import random
import time
from datetime import date, datetime

import requests

from notice_parser import NoticeData

logger = logging.getLogger(__name__)

# ── API Configuration ─────────────────────────────────────────────────
API_BASE = "https://api.openwebninja.com/realtime-zillow-data"
PROPERTY_ENDPOINT = f"{API_BASE}/property-details-address"
REQUEST_DELAY_MIN = 1.0   # seconds between requests
REQUEST_DELAY_MAX = 2.0
REQUEST_TIMEOUT = 30       # seconds per API call (Zillow can be 0.5-4s+)
MAX_RETRIES = 2

# ── Mapping tables ────────────────────────────────────────────────────

_STATUS_MAP = {
    "FOR_SALE": "Active",
    "PENDING": "Pending",
    "RECENTLY_SOLD": "Sold",
    "SOLD": "Sold",
    "FOR_RENT": "For Rent",
    "OFF_MARKET": "Off Market",
    "OTHER": "Off Market",
    "AUCTION": "Off Market",
    "FORECLOSURE": "Off Market",
    "FORECLOSURE_AUCTION": "Off Market",
    "PRE_FORECLOSURE": "Off Market",
    "BANK_OWNED": "Off Market",
    "REO": "Off Market",
}

_TYPE_MAP = {
    "SINGLE_FAMILY": "Single Family",
    "CONDO": "Condo",
    "TOWNHOUSE": "Townhouse",
    "MULTI_FAMILY": "Multi-Family",
    "MANUFACTURED": "Manufactured",
    "LOT": "Land",
    "LAND": "Land",
    "APARTMENT": "Multi-Family",
}

# Approximate average 30-year fixed mortgage rate by year (Freddie Mac PMMS)
_MORTGAGE_RATES = {
    2000: 8.05, 2001: 6.97, 2002: 6.54, 2003: 5.83, 2004: 5.84,
    2005: 5.87, 2006: 6.41, 2007: 6.34, 2008: 6.03, 2009: 5.04,
    2010: 4.69, 2011: 4.45, 2012: 3.66, 2013: 3.98, 2014: 4.17,
    2015: 3.85, 2016: 3.65, 2017: 3.99, 2018: 4.54, 2019: 3.94,
    2020: 3.11, 2021: 2.96, 2022: 5.34, 2023: 6.81, 2024: 6.72,
    2025: 6.65, 2026: 6.50,
}
_DEFAULT_RATE = 5.5


# ── Equity estimation ─────────────────────────────────────────────────

def _estimate_remaining_balance(
    purchase_price: float,
    purchase_year: int,
    ltv: float = 0.80,
    term_years: int = 30,
) -> float | None:
    """Estimate remaining mortgage balance using standard amortization.

    Assumes 80% LTV at purchase, 30-year fixed rate from historical table,
    monthly payments, no refinance.
    """
    if purchase_price <= 0 or purchase_year < 1990:
        return None

    rate_annual = _MORTGAGE_RATES.get(purchase_year, _DEFAULT_RATE) / 100.0
    rate_monthly = rate_annual / 12.0
    total_payments = term_years * 12
    principal = purchase_price * ltv

    if rate_monthly == 0:
        monthly_payment = principal / total_payments
    else:
        monthly_payment = principal * (
            rate_monthly * (1 + rate_monthly) ** total_payments
        ) / ((1 + rate_monthly) ** total_payments - 1)

    # Months elapsed since purchase (approximate mid-year)
    today = date.today()
    purchase_date = date(purchase_year, 7, 1)
    months_elapsed = (today.year - purchase_date.year) * 12 + (today.month - purchase_date.month)
    months_elapsed = max(0, min(months_elapsed, total_payments))

    if months_elapsed >= total_payments:
        return 0.0

    if rate_monthly == 0:
        remaining = principal - (monthly_payment * months_elapsed)
    else:
        remaining = principal * (
            (1 + rate_monthly) ** total_payments - (1 + rate_monthly) ** months_elapsed
        ) / ((1 + rate_monthly) ** total_payments - 1)

    return max(0.0, remaining)


# ── API call ──────────────────────────────────────────────────────────

def _fetch_property(address: str, city: str, state: str, zip_code: str,
                    api_key: str) -> dict | None:
    """Call the Zillow API for a single property. Returns parsed JSON or None."""
    parts = [address]
    if city:
        parts.append(city)
    if state:
        parts.append(state)
    if zip_code:
        parts.append(zip_code)
    # OpenWeb Ninja expects spaces between parts, NOT commas
    full_address = " ".join(parts)

    headers = {"x-api-key": api_key}
    params = {"address": full_address}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                PROPERTY_ENDPOINT,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 404:
                logger.debug("Zillow: no data for '%s'", full_address)
                return None
            if resp.status_code == 429:
                logger.warning("Zillow rate limit hit -- waiting 10s (attempt %d)", attempt)
                time.sleep(10)
                continue
            resp.raise_for_status()
            body = resp.json()
            # OpenWeb Ninja wraps response in {"status": "OK", "data": {...}}
            if body.get("status") == "OK" and body.get("data"):
                return body["data"]
            logger.debug("Zillow: empty/error response for '%s': %s", full_address, body.get("status"))
            return None
        except requests.Timeout:
            logger.warning("Zillow timeout for '%s' (attempt %d/%d)", full_address, attempt, MAX_RETRIES)
        except requests.RequestException as e:
            logger.warning("Zillow API error for '%s': %s (attempt %d/%d)", full_address, e, attempt, MAX_RETRIES)

    return None


# ── Response mapping ──────────────────────────────────────────────────

def _extract_last_sold(price_history: list[dict]) -> tuple[str, str]:
    """Find the most recent 'Sold' event from priceHistory.

    Returns (sold_date_iso, sold_price_str) or ("", "").
    """
    for entry in price_history:
        event = (entry.get("event") or "").strip()
        if event.lower() in ("sold", "listed (sold)"):
            raw_date = entry.get("date", "")
            price = entry.get("price")
            date_str = ""
            if raw_date:
                try:
                    dt = datetime.strptime(str(raw_date)[:10], "%Y-%m-%d")
                    date_str = dt.strftime("%Y-%m-%d")
                except ValueError:
                    date_str = str(raw_date)[:10]
            price_str = str(int(price)) if price else ""
            return date_str, price_str
    return "", ""


def _get_listing_price(data: dict, status: str) -> str:
    """Determine listing price based on MLS status."""
    if status in ("Active", "Pending"):
        price = data.get("price")
        if price:
            return str(int(price))
    elif status == "Sold":
        price_history = data.get("priceHistory") or []
        _, sold_price = _extract_last_sold(price_history)
        return sold_price
    return ""


def _normalize_lot_size(data: dict) -> str:
    """Convert lot size to square feet string."""
    value = data.get("lotAreaValue")
    units = (data.get("lotAreaUnits") or data.get("lotAreaUnit") or "").lower()
    if not value:
        return ""
    try:
        val = float(value)
    except (TypeError, ValueError):
        return ""
    if "acre" in units:
        val = val * 43560
    return str(int(val))


_AUCTION_SUBTYPE_KEYS = (
    # snake_case (listing_sub_type dict)
    "is_forAuction",
    "is_foreclosure",
    "is_bankOwned",
    "is_preForeclosure",
    # camelCase (listingSubType dict)
    "isForAuction",
    "isForeclosure",
    "isBankOwned",
)


def _is_auction_listing(data: dict) -> bool:
    subtype = data.get("listingSubType") or data.get("listing_sub_type") or {}
    if isinstance(subtype, dict):
        for key in _AUCTION_SUBTYPE_KEYS:
            if subtype.get(key):
                return True
    for key in _AUCTION_SUBTYPE_KEYS:
        if data.get(key):
            return True
    return False


def _apply_property_data(notice: NoticeData, data: dict) -> bool:
    """Map Zillow response fields onto a NoticeData object (in-place).

    Returns True if enrichment was successful (at least homeStatus or zestimate found).
    """
    # MLS status -- auction/foreclosure listings are NOT on the MLS
    raw_status = data.get("homeStatus") or ""
    if _is_auction_listing(data):
        notice.mls_status = "Off Market"
    else:
        notice.mls_status = _STATUS_MAP.get(raw_status.upper(), raw_status.replace("_", " ").title())

    # Listing price
    notice.mls_listing_price = _get_listing_price(data, notice.mls_status)

    # Price history -- last sold
    price_history = data.get("priceHistory") or []
    sold_date, sold_price = _extract_last_sold(price_history)
    notice.mls_last_sold_date = sold_date
    notice.mls_last_sold_price = sold_price

    # Zestimate
    zestimate = data.get("zestimate")
    if zestimate:
        notice.estimated_value = str(int(zestimate))

    # Property characteristics
    home_type = data.get("homeType") or ""
    notice.property_type = _TYPE_MAP.get(home_type.upper(), home_type.replace("_", " ").title())

    notice.bedrooms = str(data.get("bedrooms") or "") if data.get("bedrooms") else ""
    notice.bathrooms = str(data.get("bathrooms") or "") if data.get("bathrooms") else ""

    living_area = data.get("livingArea")
    notice.sqft = str(int(living_area)) if living_area else ""

    year_built = data.get("yearBuilt")
    notice.year_built = str(year_built) if year_built else ""

    notice.lot_size = _normalize_lot_size(data)

    # Equity estimation
    if zestimate and sold_price and sold_date:
        try:
            purchase_year = int(sold_date[:4])
            purchase_price = float(sold_price)
            remaining = _estimate_remaining_balance(purchase_price, purchase_year)
            if remaining is not None:
                equity = float(zestimate) - remaining
                notice.estimated_equity = str(int(equity))
                if float(zestimate) > 0:
                    pct = (equity / float(zestimate)) * 100
                    notice.equity_percent = f"{pct:.1f}"
        except (ValueError, ZeroDivisionError):
            pass

    return bool(data.get("homeStatus") or zestimate)


# ── Main entry point ──────────────────────────────────────────────────

def enrich_properties(
    notices: list[NoticeData],
    api_key: str,
) -> list[NoticeData]:
    """Enrich notices with Zillow property data (in-place).

    Args:
        notices: List of NoticeData (modified in-place).
        api_key: OpenWeb Ninja API key for Real-Time Zillow Data API.

    Returns:
        The same list (modified in-place) for chaining convenience.
    """
    if not api_key:
        logger.info("OpenWeb Ninja API key not configured -- skipping Zillow enrichment")
        return notices

    eligible = [(i, n) for i, n in enumerate(notices) if n.address.strip()]
    if not eligible:
        logger.info("No notices with addresses to enrich")
        return notices

    logger.info(
        "Enriching %d properties via Zillow API (%d skipped -- no address)",
        len(eligible),
        len(notices) - len(eligible),
    )

    enriched = 0
    failed = 0
    skipped = len(notices) - len(eligible)
    equity_values: list[float] = []

    for idx, (orig_idx, notice) in enumerate(eligible):
        if idx > 0:
            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            time.sleep(delay)

        data = _fetch_property(
            notice.address, notice.city, notice.state, notice.zip,
            api_key,
        )

        if data is None:
            failed += 1
            continue

        success = _apply_property_data(notice, data)
        if success:
            enriched += 1
            if notice.estimated_equity:
                try:
                    equity_values.append(float(notice.estimated_equity))
                except ValueError:
                    pass
        else:
            failed += 1

        if (idx + 1) % 10 == 0:
            logger.info(
                "Zillow enrichment progress: %d/%d (enriched=%d, failed=%d)",
                idx + 1, len(eligible), enriched, failed,
            )

    avg_equity = ""
    if equity_values:
        avg = sum(equity_values) / len(equity_values)
        avg_equity = f", avg equity=${avg:,.0f}"
    logger.info(
        "Zillow enrichment complete: %d enriched, %d failed, %d skipped%s",
        enriched, failed, skipped, avg_equity,
    )

    return notices
