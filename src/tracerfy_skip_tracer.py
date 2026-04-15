"""Tracerfy batch skip trace — phones + emails for all records.

Submits all records to POST /v1/api/trace/ (batch endpoint, $0.02/record),
polls for results, and populates NoticeData phone/email fields.
Runs as a separate pipeline step before DataSift CSV generation.

Signing chain support: traces ALL signing-authority heirs (not just DM #1)
so the user has full contact info for every heir who must sign to close a deal.
"""

import csv
import io
import json
import logging
import time

import requests

import config as cfg
from notice_parser import NoticeData

logger = logging.getLogger(__name__)

# Tracerfy batch response phone/email fields
PHONE_FIELDS = [
    "primary_phone", "mobile_1", "mobile_2", "mobile_3", "mobile_4",
    "mobile_5", "landline_1", "landline_2", "landline_3",
]
EMAIL_FIELDS = ["email_1", "email_2", "email_3", "email_4", "email_5"]

TRACERFY_TRACE_URL = "https://tracerfy.com/v1/api/trace/"
TRACERFY_QUEUE_URL = "https://tracerfy.com/v1/api/queue/"


def _get_contacts_for_trace(
    notice: NoticeData, max_signing_traces: int = 5,
) -> list[tuple[str, str, str, str, str, str]]:
    """Determine who to skip-trace for this notice.

    Returns list of (first_name, last_name, address, city, zip, heir_key).
    heir_key is the full name used to match results back to the right heir.

    For deceased owners: traces DM #1 + all signing-authority heirs with addresses.
    For living owners: traces the property owner only.
    """
    contacts = []

    if (notice.owner_deceased == "yes"
            and notice.decision_maker_name
            and notice.decision_maker_name.strip()):

        # Always include DM #1 (primary contact)
        dm_name = notice.decision_maker_name.strip()
        address = notice.decision_maker_street or notice.address or ""
        city_val = notice.decision_maker_city or notice.city or ""
        zip_code = notice.decision_maker_zip or notice.zip or ""
        first, last = _split_name(dm_name)
        if first and last:
            contacts.append((first, last, address, city_val, zip_code, dm_name))

        # Add other signing-authority heirs from heir_map_json
        if notice.heir_map_json:
            try:
                heirs = json.loads(notice.heir_map_json)
            except (json.JSONDecodeError, TypeError):
                heirs = []

            seen = {dm_name.lower()}
            for heir in heirs:
                if len(contacts) >= max_signing_traces:
                    break
                heir_name = heir.get("name", "").strip()
                if not heir_name or heir_name.lower() in seen:
                    continue
                if not heir.get("signing_authority"):
                    continue
                if heir.get("status") == "deceased":
                    continue
                if not heir.get("street"):
                    continue  # No address = can't trace effectively
                seen.add(heir_name.lower())
                h_first, h_last = _split_name(heir_name)
                if h_first and h_last:
                    contacts.append((
                        h_first, h_last,
                        heir["street"],
                        heir.get("city", ""),
                        heir.get("zip", ""),
                        heir_name,
                    ))
    else:
        # Living owner — single contact
        name = (notice.owner_name or "").strip()
        if name:
            first, last = _split_name(name)
            if first and last:
                contacts.append((
                    first, last,
                    notice.address or "",
                    notice.city or "",
                    notice.zip or "",
                    name,
                ))

    return contacts


def _split_name(name: str) -> tuple[str, str]:
    """Split a full name into (first, last). Returns ('', '') if unparseable."""
    parts = name.strip().split()
    if len(parts) < 2:
        return ("", "")
    return (parts[0], parts[-1])


# Keep backward-compatible single-contact function for callers that expect it
def _get_contact_for_trace(notice: NoticeData) -> tuple[str, str, str, str, str]:
    """Legacy single-contact wrapper. Returns (first, last, address, city, zip)."""
    contacts = _get_contacts_for_trace(notice, max_signing_traces=1)
    if contacts:
        first, last, addr, city, zip_code, _ = contacts[0]
        return (first, last, addr, city, zip_code)
    return ("", "", "", "", "")


def _lookup_missing_heir_addresses(
    notice: NoticeData, api_key: str | None,
) -> int:
    """Fill in mailing addresses for signing-authority heirs that lack one.

    For each living heir with signing_authority=true but no `street`, runs the
    existing DM address waterfall (Knox Tax → Serper/Firecrawl → DDG) and stores
    the result back onto the heir. Mutates notice.heir_map_json in place.

    Returns the number of heirs that gained an address.
    """
    if not notice.heir_map_json:
        return 0
    try:
        heirs = json.loads(notice.heir_map_json)
    except (json.JSONDecodeError, TypeError):
        return 0
    if not isinstance(heirs, list):
        return 0

    # Lazy import to avoid a hard dependency cycle on obituary_enricher
    from obituary_enricher import _lookup_dm_address

    city_hint = (notice.city or "").strip()
    filled = 0
    for heir in heirs:
        if not isinstance(heir, dict):
            continue
        if not heir.get("signing_authority"):
            continue
        if heir.get("status") == "deceased":
            continue
        if (heir.get("street") or "").strip():
            continue
        heir_name = (heir.get("name") or "").strip()
        if not heir_name:
            continue

        try:
            addr = _lookup_dm_address(
                heir_name, city_hint, api_key or "", tracerfy_tier1=False,
            )
        except Exception as e:
            logger.debug("Heir address lookup failed for %s: %s", heir_name, e)
            continue
        if addr and addr.get("street"):
            heir["street"] = addr.get("street", "")
            heir["city"] = addr.get("city", "") or city_hint
            heir["state"] = addr.get("state", "") or "TN"
            heir["zip"] = addr.get("zip", "")
            heir["address_source"] = addr.get("source", "")
            filled += 1
            logger.info(
                "  Heir address filled: %s → %s, %s",
                heir_name, heir["street"], heir.get("city", ""),
            )

    if filled:
        notice.heir_map_json = json.dumps(heirs, ensure_ascii=False)
    return filled


def batch_skip_trace(
    notices: list[NoticeData],
    max_signing_traces: int = 5,
    lookup_heir_addresses: bool = True,
    address_lookup_api_key: str | None = None,
) -> dict:
    """Run Tracerfy batch skip trace on all records.

    Submits a single batch CSV to POST /v1/api/trace/, polls for results,
    and populates phone/email fields on each NoticeData object.

    For deceased owners, traces ALL signing-authority heirs (up to max_signing_traces
    per property). DM #1's phones go to flat NoticeData fields; other heirs'
    phones/emails are stored in their heir_map_json entry.

    When lookup_heir_addresses is True, signing-authority heirs without a known
    mailing address get one looked up (Knox Tax → people search) before the trace
    so Tracerfy has enough info to return phones. Uses ANTHROPIC_API_KEY (or the
    explicit override) for LLM-based extraction from people-search pages.

    Returns stats dict: {total, submitted, matched, phones_found, emails_found,
                         cost, signing_heirs_traced, heir_addresses_filled}.
    """
    stats = {
        "total": len(notices),
        "submitted": 0,
        "matched": 0,
        "phones_found": 0,
        "emails_found": 0,
        "cost": 0.0,
        "signing_heirs_traced": 0,
        "heir_addresses_filled": 0,
    }

    if not cfg.TRACERFY_API_KEY:
        logger.warning("Tracerfy API key not set — skipping batch skip trace")
        return stats

    # Fill missing heir addresses BEFORE building the trace batch — otherwise
    # those heirs get silently dropped at the `if not heir.get("street")` check
    # in _get_contacts_for_trace and never get Tracerfy phones.
    if lookup_heir_addresses:
        llm_key = address_lookup_api_key or getattr(cfg, "ANTHROPIC_API_KEY", "") or None
        for notice in notices:
            if notice.owner_deceased != "yes":
                continue
            try:
                stats["heir_addresses_filled"] += _lookup_missing_heir_addresses(notice, llm_key)
            except Exception:
                logger.exception("Heir address lookup pass failed for notice")
        if stats["heir_addresses_filled"]:
            logger.info("Heir address backfill: %d heir(s) gained an address",
                        stats["heir_addresses_filled"])

    # Build lookup map: list of (notice, first, last, address, city, zip, heir_key)
    # Multiple entries per notice for signing-authority heirs
    lookup_map: list[tuple[NoticeData, str, str, str, str, str, str]] = []
    for notice in notices:
        # Skip records that already have phone data (DM #1)
        contacts = _get_contacts_for_trace(notice, max_signing_traces)
        for i, (first, last, address, city, zip_code, heir_key) in enumerate(contacts):
            # Skip DM #1 if already has phones
            if i == 0 and notice.primary_phone:
                continue
            # Skip heirs already traced (have phones in heir_map_json)
            if i > 0 and _heir_has_phones(notice, heir_key):
                continue
            lookup_map.append((notice, first, last, address, city, zip_code, heir_key))

    if not lookup_map:
        logger.info("Tracerfy: no records to skip-trace (all have phones or no valid names)")
        return stats

    stats["submitted"] = len(lookup_map)
    stats["signing_heirs_traced"] = sum(
        1 for n, _, _, _, _, _, hk in lookup_map
        if n.decision_maker_name and hk != n.decision_maker_name
    )
    logger.info("Tracerfy batch: submitting %d contacts (%d notices, %d signing heirs) — $%.2f",
                len(lookup_map),
                len(set(id(n) for n, *_ in lookup_map)),
                stats["signing_heirs_traced"],
                len(lookup_map) * 0.02)

    # Build in-memory CSV
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["first_name", "last_name", "address", "city", "state",
                     "zip", "mail_address", "mail_city", "mail_state"])
    for notice_ref, first, last, address, city, zip_code, _ in lookup_map:
        state = notice_ref.state or "TN"
        writer.writerow([first, last, address, city, state, zip_code, "", "", ""])
    csv_content = csv_buffer.getvalue()
    csv_buffer.close()

    try:
        # Submit batch trace job
        resp = requests.post(
            TRACERFY_TRACE_URL,
            headers={"Authorization": f"Bearer {cfg.TRACERFY_API_KEY}"},
            data={
                "first_name_column": "first_name",
                "last_name_column": "last_name",
                "address_column": "address",
                "city_column": "city",
                "state_column": "state",
                "zip_column": "zip",
                "mail_address_column": "mail_address",
                "mail_city_column": "mail_city",
                "mail_state_column": "mail_state",
                "mailing_zip_column": "zip",
            },
            files={"csv_file": ("skip_trace_batch.csv", csv_content, "text/csv")},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning("Tracerfy batch %d response: %s",
                           resp.status_code, resp.text[:500])
        resp.raise_for_status()
        queue_data = resp.json()
        queue_id = queue_data.get("queue_id")
        if not queue_id:
            logger.warning("Tracerfy batch returned no queue_id")
            return stats

        est_wait = queue_data.get("estimated_wait_seconds", "unknown")
        logger.info("  Tracerfy batch job %s submitted (est. %ss)", queue_id, est_wait)

        # Poll for results (up to 5 minutes)
        for attempt in range(60):
            time.sleep(5)
            result_resp = requests.get(
                f"{TRACERFY_QUEUE_URL}{queue_id}",
                headers={"Authorization": f"Bearer {cfg.TRACERFY_API_KEY}"},
                timeout=15,
            )
            result_resp.raise_for_status()
            result_data = result_resp.json()

            # Handle both response formats
            if isinstance(result_data, list):
                records = result_data
            elif isinstance(result_data, dict):
                status = result_data.get("status", "")
                if status == "failed":
                    logger.warning("Tracerfy batch job %s failed", queue_id)
                    return stats
                if status != "completed":
                    if attempt % 6 == 5:
                        logger.info("  Tracerfy batch still processing (%ds)...",
                                    (attempt + 1) * 5)
                    continue
                records = result_data.get("records", [])
            else:
                continue

            # Match results back to notices
            _match_results(records, lookup_map, stats)
            stats["cost"] = stats["submitted"] * 0.02
            logger.info("  Tracerfy batch complete: %d/%d matched, %d phones, %d emails, $%.2f",
                        stats["matched"], stats["submitted"],
                        stats["phones_found"], stats["emails_found"], stats["cost"])
            return stats

        logger.warning("Tracerfy batch job %s timed out after 5 min", queue_id)
        stats["cost"] = stats["submitted"] * 0.02  # Still charged
        return stats

    except Exception as e:
        logger.warning("Tracerfy batch skip trace failed: %s", e)
        return stats


def _heir_has_phones(notice: NoticeData, heir_key: str) -> bool:
    """Check if a specific heir already has phone data in heir_map_json."""
    if not notice.heir_map_json:
        return False
    try:
        heirs = json.loads(notice.heir_map_json)
        for h in heirs:
            if h.get("name", "").lower() == heir_key.lower():
                return bool(h.get("phones"))
    except (json.JSONDecodeError, TypeError):
        pass
    return False


def _match_results(records: list, lookup_map: list, stats: dict) -> None:
    """Match Tracerfy batch response records back to NoticeData objects.

    DM #1's phones/emails go to flat NoticeData fields (backward compat).
    Other signing heirs' phones/emails go into their heir_map_json entry.
    """
    for rec in records:
        if not isinstance(rec, dict):
            continue

        rec_first = (rec.get("first_name") or "").strip().lower()
        rec_last = (rec.get("last_name") or "").strip().lower()
        if not rec_first or not rec_last:
            continue

        # Find matching entry in lookup_map
        for notice, first, last, address, city, zip_code, heir_key in lookup_map:
            if first.lower() != rec_first or last.lower() != rec_last:
                continue

            # Extract phones and emails from response
            phones = []
            for field in PHONE_FIELDS:
                value = (rec.get(field) or "").strip()
                if value:
                    phones.append(value)

            emails = []
            for field in EMAIL_FIELDS:
                value = (rec.get(field) or "").strip()
                if value:
                    emails.append(value)

            if not phones and not emails:
                break

            # Is this the primary DM (#1)?
            is_primary = (
                notice.decision_maker_name
                and heir_key.lower() == notice.decision_maker_name.strip().lower()
            ) or notice.owner_deceased != "yes"

            if is_primary and not notice.primary_phone:
                # Populate flat NoticeData phone/email fields (backward compat)
                for i, field in enumerate(PHONE_FIELDS):
                    if i < len(phones):
                        setattr(notice, field, phones[i])
                for i, field in enumerate(EMAIL_FIELDS):
                    if i < len(emails):
                        setattr(notice, field, emails[i])
            elif not is_primary:
                # Store on the heir's entry in heir_map_json
                _store_heir_phones(notice, heir_key, phones, emails)

            stats["matched"] += 1
            stats["phones_found"] += len(phones)
            stats["emails_found"] += len(emails)
            logger.info("    %s %s: %d phones, %d emails%s",
                        first, last, len(phones), len(emails),
                        " (signing heir)" if not is_primary else "")
            break


def _store_heir_phones(
    notice: NoticeData, heir_key: str,
    phones: list[str], emails: list[str],
) -> None:
    """Store phones/emails on a specific heir's entry in heir_map_json."""
    if not notice.heir_map_json:
        return
    try:
        heirs = json.loads(notice.heir_map_json)
        for h in heirs:
            if h.get("name", "").lower() == heir_key.lower():
                h["phones"] = phones
                h["emails"] = emails
                break
        notice.heir_map_json = json.dumps(heirs, ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        pass
