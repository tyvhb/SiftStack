"""Extract Market Finder data from DataSift.ai via Playwright automation.

Logs into DataSift, navigates to Market Finder, selects state/county,
and extracts ALL ZIP code + neighborhood data tables plus summary panel.

IMPORTANT: Always extracts EVERY row — scrolls through infinite-scroll
tables until no new rows appear. Never truncate or stop at visible rows.

Usage:
    python src/extract_market_finder.py --state "Tennessee" --county "Knox"
    python src/extract_market_finder.py --state "Tennessee" --county "Knox" --headless
    python src/extract_market_finder.py --state "Tennessee" --county "Knox,Blount"

Output: JSON file(s) in --output-dir with extracted market data.

Requires: playwright, python-dotenv
          DATASIFT_EMAIL and DATASIFT_PASSWORD in .env or environment
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from datasift_core import (
    create_browser,
    login,
    dismiss_popups,
    screenshot,
    wait_for_spa,
    DATASIFT_MARKET_FINDER_URL,
)

logger = logging.getLogger(__name__)

# ── Column Definitions ────────────────────────────────────────────────

ZIP_COLUMNS = [
    "zip_code",
    "total_inv_trans_6mo",
    "homes_on_market",
    "homes_sold_last_month",
    "median_days_on_market",
    "median_home_value",
    "median_sale_price",
]

NEIGHBORHOOD_COLUMNS = [
    "neighborhood",
    "total_inv_trans_6mo",
    "homes_on_market",
    "homes_sold_last_month",
    "median_days_on_market",
    "median_home_value",
    "median_sale_price",
]


def _parse_currency(value: str) -> float | None:
    """Parse currency string like '$304,569' to float."""
    if not value:
        return None
    cleaned = value.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_int(value: str) -> int | None:
    """Parse integer string, stripping commas."""
    if not value:
        return None
    cleaned = value.replace(",", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        return None


def _row_to_dict(row: list[str], columns: list[str]) -> dict:
    """Convert a raw table row to a named dict with parsed types."""
    d = {}
    for i, col in enumerate(columns):
        val = row[i] if i < len(row) else ""
        if col in ("median_home_value", "median_sale_price"):
            d[col] = _parse_currency(val)
        elif col in ("zip_code", "neighborhood"):
            d[col] = val
        else:
            d[col] = _parse_int(val)
        d[f"{col}_raw"] = val
    return d


# ── Aggressive Popup Dismissal ───────────────────────────────────────

async def _dismiss_all_popups(page) -> None:
    """Aggressively dismiss ALL popups, overlays, and NPS surveys.

    Must be called before ANY interaction — Beamer NPS iframe blocks
    ALL pointer events globally.
    """
    await dismiss_popups(page)

    # Extra: remove any remaining fixed/absolute overlays and iframes
    await page.evaluate("""() => {
        // Remove Beamer NPS survey (blocks ALL pointer events)
        const nps = document.getElementById('npsIframeContainer');
        if (nps) nps.remove();

        // Remove all Beamer elements
        document.querySelectorAll(
            '[id*="beamer"], [class*="beamer"], [id*="nps"], [class*="nps"]'
        ).forEach(el => el.remove());

        // Remove any blocking iframes
        document.querySelectorAll('iframe').forEach(f => {
            const style = getComputedStyle(f);
            if (style.position === 'fixed' || style.position === 'absolute') {
                f.remove();
            }
        });

        // Remove fullscreen overlays that intercept clicks
        document.querySelectorAll('[style*="position: fixed"], [style*="position:fixed"]').forEach(el => {
            const rect = el.getBoundingClientRect();
            // Only remove if it covers a large area (likely an overlay)
            if (rect.width > 300 && rect.height > 200) {
                const text = el.textContent || '';
                if (text.includes('recommend') || text.includes('survey') ||
                    text.includes('notification') || text.includes('How likely')) {
                    el.remove();
                }
            }
        });
    }""")
    await page.wait_for_timeout(500)


# ── Navigation ────────────────────────────────────────────────────────

async def _navigate_to_market_finder(page) -> bool:
    """Navigate to Market Finder page."""
    await page.goto(DATASIFT_MARKET_FINDER_URL, wait_until="domcontentloaded")
    await wait_for_spa(page, 5000)
    await _dismiss_all_popups(page)

    # Check if we landed on Market Finder
    mf_check = await page.evaluate("""() => {
        return document.body.innerText.includes('Market Finder') ||
               document.body.innerText.includes('Investor Transactions') ||
               document.querySelector('[class*="MarketFinder"]') !== null;
    }""")

    if mf_check:
        logger.info("Navigated to Market Finder via direct URL")
        return True

    # Fallback: click sidebar link
    logger.info("Direct URL didn't land on Market Finder, trying sidebar")
    sidebar_link = page.get_by_text("Market Finder", exact=False)
    if await sidebar_link.count() > 0:
        await sidebar_link.first.click()
        await wait_for_spa(page, 5000)
        await _dismiss_all_popups(page)
        logger.info("Navigated to Market Finder via sidebar link")
        return True

    logger.error("Could not navigate to Market Finder")
    return False


async def _select_state(page, state: str) -> bool:
    """Select a state from the Market Finder state dropdown.

    DataSift uses styled-components dropdowns (no native <select>).
    Strategy: find the first dropdown in the Market Finder area,
    click it, search/select the state.
    """
    await _dismiss_all_popups(page)

    try:
        # Strategy 1: Find styled-component Select dropdowns in the main area
        # Use JS to enumerate all Select-like elements and click the first one
        found = await page.evaluate("""(state) => {
            // Find all dropdown-like elements
            const candidates = [
                ...document.querySelectorAll('[class*="Selectstyles__Select"]'),
                ...document.querySelectorAll('[class*="SelectValue"]'),
                ...document.querySelectorAll('[class*="select-container"]'),
                ...document.querySelectorAll('[class*="dropdown"]'),
                ...document.querySelectorAll('[class*="Select__"]'),
            ];

            // Filter to ones in the main content area (x > 200 to skip sidebar)
            const mainCandidates = candidates.filter(el => {
                const rect = el.getBoundingClientRect();
                return rect.x > 200 && rect.width > 80 && rect.height > 20;
            });

            // Return info about what we found
            return {
                total: candidates.length,
                main: mainCandidates.length,
                texts: mainCandidates.slice(0, 5).map(el => ({
                    text: el.innerText.trim().substring(0, 50),
                    class: el.className.substring(0, 80),
                    x: Math.round(el.getBoundingClientRect().x),
                    y: Math.round(el.getBoundingClientRect().y),
                }))
            };
        }""", state)
        logger.info("Found %d total dropdown candidates, %d in main area: %s",
                     found["total"], found["main"], json.dumps(found["texts"], indent=2))

        # Strategy 2: Try clicking the first dropdown in the top area
        # Look for the search/filter bar near the top
        search_area = await page.evaluate("""() => {
            // Find text inputs or search boxes in the Market Finder area
            const inputs = document.querySelectorAll('input[type="text"], input[type="search"], input:not([type])');
            const results = [];
            for (const inp of inputs) {
                const rect = inp.getBoundingClientRect();
                if (rect.x > 200 && rect.y < 200) {
                    results.push({
                        placeholder: inp.placeholder || '',
                        value: inp.value || '',
                        x: Math.round(rect.x),
                        y: Math.round(rect.y),
                        class: inp.className.substring(0, 80),
                    });
                }
            }
            return results;
        }""")
        logger.info("Found %d search inputs in top area: %s", len(search_area), json.dumps(search_area, indent=2))

        # Strategy 3: Try to find and click "Search Screen" or state selection area
        # Look for the top controls area
        top_clicks = [
            'text="Search Screen"',
            'text="Select States"',
            'text="States"',
            '[placeholder*="state" i]',
            '[placeholder*="search" i]',
            '[placeholder*="Search" i]',
        ]

        for selector in top_clicks:
            el = page.locator(selector)
            if await el.count() > 0:
                logger.info("Found element via '%s', clicking...", selector)
                await el.first.click(force=True)
                await page.wait_for_timeout(1500)
                await _dismiss_all_popups(page)
                break

        # Strategy 4: Try to use any visible input/search box to type the state
        # After clicking, look for a text input where we can type
        for selector in [
            'input[placeholder*="search" i]',
            'input[placeholder*="state" i]',
            'input:not([type="hidden"])',
            '[class*="SearchInput"] input',
            '[class*="search"] input',
        ]:
            inp = page.locator(selector)
            visible_inputs = []
            for i in range(await inp.count()):
                if await inp.nth(i).is_visible():
                    visible_inputs.append(inp.nth(i))

            if visible_inputs:
                logger.info("Found visible input via '%s', typing state '%s'", selector, state)
                await visible_inputs[0].fill(state)
                await page.wait_for_timeout(1000)

                # Look for the state in the dropdown options
                state_option = page.get_by_text(state, exact=True)
                if await state_option.count() > 0:
                    await state_option.first.click()
                    await page.wait_for_timeout(3000)
                    logger.info("Selected state: %s", state)
                    return True

                # Try partial match
                state_option = page.locator(f'[class*="Option"]:has-text("{state}")')
                if await state_option.count() > 0:
                    await state_option.first.click()
                    await page.wait_for_timeout(3000)
                    logger.info("Selected state via option class: %s", state)
                    return True

                # Clear and try next input
                await visible_inputs[0].fill("")
                await page.wait_for_timeout(500)

        # Strategy 5: Click on the state on the map
        logger.info("Trying to click on Tennessee on the US map")
        tn_on_map = page.locator('path[data-name="Tennessee"], path[title="Tennessee"]')
        if await tn_on_map.count() > 0:
            await tn_on_map.first.click()
            await page.wait_for_timeout(3000)
            logger.info("Clicked Tennessee on map")
            return True

        # Strategy 6: Try using JS to click by evaluating all clickable elements
        clicked = await page.evaluate("""(state) => {
            // Find any element containing the state name that looks clickable
            const all = document.querySelectorAll('*');
            for (const el of all) {
                if (el.children.length === 0 && el.textContent.trim() === state) {
                    const rect = el.getBoundingClientRect();
                    if (rect.x > 200 && rect.width > 20) {
                        el.click();
                        return { clicked: true, tag: el.tagName, x: rect.x, y: rect.y };
                    }
                }
            }
            return { clicked: false };
        }""", state)

        if clicked.get("clicked"):
            await page.wait_for_timeout(3000)
            logger.info("Clicked state element via JS: %s", clicked)
            return True

        logger.error("All strategies failed to select state '%s'", state)
        await screenshot(page, "market_finder_state_debug")
        return False

    except Exception as e:
        logger.error("Failed to select state '%s': %s", state, e)
        await screenshot(page, "market_finder_state_error")
        return False


async def _select_county(page, county: str) -> bool:
    """Select a county using the 'Select Counties' input in the top filter bar.

    The Market Finder top bar has: Select States | Select Counties | ZIP Codes | ...
    Each is an InputMultiSearch with a specific placeholder. We target the
    'Select Counties' input directly, type the county name, and click the
    matching dropdown option.
    """
    await _dismiss_all_popups(page)

    try:
        # Wait for state selection to settle
        await page.wait_for_timeout(2000)

        # Target the Select Counties input specifically by placeholder
        county_input = page.locator('[placeholder="Select Counties"]')
        if await county_input.count() == 0:
            # Fallback: try case-insensitive
            county_input = page.locator('[placeholder*="county" i]')

        if await county_input.count() == 0:
            logger.error("Select Counties input not found")
            await screenshot(page, "market_finder_county_input_missing")
            return False

        logger.info("Found Select Counties input, clicking and typing '%s'", county)
        await county_input.first.click(force=True)
        await page.wait_for_timeout(1000)

        # Type the county name to filter the dropdown
        await county_input.first.fill(county)
        await page.wait_for_timeout(1500)

        # Click the matching dropdown option
        # Look for options in dropdown containers
        option_clicked = False
        for selector in [
            f'[class*="Option"]:has-text("{county}")',
            f'[class*="option"]:has-text("{county}")',
            f'[class*="Result"]:has-text("{county}")',
            f'[class*="Item"]:has-text("{county}")',
            f'li:has-text("{county}")',
        ]:
            opt = page.locator(selector)
            if await opt.count() > 0:
                await opt.first.click()
                option_clicked = True
                logger.info("Clicked county option via '%s'", selector)
                break

        if not option_clicked:
            # Try clicking any element with the county text that's in a dropdown
            county_text = page.get_by_text(county, exact=False)
            for i in range(min(await county_text.count(), 5)):
                el = county_text.nth(i)
                bbox = await el.bounding_box()
                # Dropdown options should appear below the input bar (y > 120)
                # and in the main area (x > 200)
                if bbox and bbox["y"] > 120 and bbox["x"] > 200 and bbox["height"] < 60:
                    await el.click()
                    option_clicked = True
                    logger.info("Clicked county dropdown option at y=%d", int(bbox["y"]))
                    break

        if not option_clicked:
            logger.error("County option '%s' not found in dropdown", county)
            await screenshot(page, "market_finder_county_dropdown_empty")
            return False

        # Wait for the view to transition from state-level to county-level
        await page.wait_for_timeout(5000)

        # Verify: check if table now shows ZIP codes instead of counties
        verify = await page.evaluate("""() => {
            const cells = document.querySelectorAll('td, [class*="Cell"]');
            for (const cell of cells) {
                const text = cell.innerText.trim();
                // ZIP codes are 5-digit numbers
                if (/^\\d{5}$/.test(text)) return { hasZips: true, sampleZip: text };
            }
            return { hasZips: false };
        }""")

        if verify.get("hasZips"):
            logger.info("County selected — table now shows ZIP codes (sample: %s)", verify.get("sampleZip"))
        else:
            logger.warning("County may not have loaded — no ZIP codes visible in table yet")
            # Give extra time
            await page.wait_for_timeout(3000)

        logger.info("Selected county: %s", county)
        return True

    except Exception as e:
        logger.error("Failed to select county '%s': %s", county, e)
        await screenshot(page, "market_finder_county_error")
        return False


# ── Data Extraction ───────────────────────────────────────────────────

async def _extract_all_table_rows(page) -> list[list[str]]:
    """Extract ALL rows from the visible data table via infinite scroll.

    CRITICAL: Scrolls until no new rows appear to ensure EVERY ZIP/neighborhood
    is captured. Never stops at a partial result.

    The Market Finder table has 7 columns:
    NAME | TOTAL INV. TRANS. FOR 6 MO. | HOMES ON MARKET | HOMES SOLD LAST MONTH |
    MEDIAN DAYS ON MARKET | MEDIAN HOME VALUE | MEDIAN SALE PRICE
    """
    all_rows = []
    seen_keys = set()
    prev_count = 0
    stale_rounds = 0

    for attempt in range(100):  # Up to 100 scroll attempts for large counties
        # Extract visible rows via JS — use <td> elements only to avoid
        # double-counting from sub-elements (spans/divs inside cells)
        data = await page.evaluate("""() => {
            const rows = [];

            // Find the main data table in the content area (x > 200)
            const tables = document.querySelectorAll('table');
            for (const table of tables) {
                const rect = table.getBoundingClientRect();
                if (rect.x < 150 || rect.width < 300) continue;

                const trs = table.querySelectorAll('tbody tr, tr');
                for (const tr of trs) {
                    // Use ONLY direct <td> children — prevents double-counting
                    // from nested elements within cells
                    const cells = tr.querySelectorAll('td');
                    if (cells.length >= 7) {
                        // Extract only the direct text content of each <td>
                        const row = Array.from(cells).map(c => c.innerText.trim());
                        rows.push(row);
                    }
                }
                if (rows.length > 0) return rows;
            }

            // Fallback: div-based table (styled-components)
            const containers = document.querySelectorAll(
                '[class*="Table"], [class*="DataTable"], [role="table"], [class*="Grid"]'
            );
            for (const container of containers) {
                const rect = container.getBoundingClientRect();
                if (rect.x < 150 || rect.width < 300) continue;

                const divRows = container.querySelectorAll(
                    '[class*="Row"]:not([class*="Header"]):not([class*="header"]), '
                    + '[role="row"]:not([role="columnheader"])'
                );
                for (const dr of divRows) {
                    // For div tables, use only direct child cells (not nested)
                    const cells = dr.querySelectorAll(':scope > [class*="Cell"], :scope > [role="cell"], :scope > td');
                    if (cells.length >= 7) {
                        const row = Array.from(cells).map(c => c.innerText.trim());
                        if (row.some(v => v.length > 0)) rows.push(row);
                    }
                }
            }
            return rows;
        }""")

        for row in data:
            # Use first cell as dedup key (ZIP code or neighborhood name)
            key = row[0] if row else ""
            if key and key not in seen_keys and key.upper() not in ("ZIP CODE", "ZIP", "NEIGHBORHOOD", ""):
                seen_keys.add(key)
                all_rows.append(row)

        if len(all_rows) == prev_count:
            stale_rounds += 1
            if stale_rounds >= 3:  # No new data after 3 consecutive scrolls
                logger.debug("No new rows after %d scrolls (total: %d)", attempt, len(all_rows))
                break
        else:
            stale_rounds = 0

        prev_count = len(all_rows)

        # Scroll the table container down to trigger lazy loading
        await page.evaluate("""() => {
            const candidates = [
                document.querySelector('[class*="TableBody"]'),
                document.querySelector('[class*="table-body"]'),
                document.querySelector('[class*="Table"]'),
                document.querySelector('[class*="DataTable"]'),
                document.querySelector('[class*="Grid"]'),
                document.querySelector('[role="table"]'),
                document.querySelector('table'),
            ].filter(Boolean);

            // Find a scrollable container
            for (const el of candidates) {
                if (el.scrollHeight > el.clientHeight + 10) {
                    el.scrollTop = el.scrollHeight;
                    return 'scrolled_container';
                }
                // Try parent
                if (el.parentElement && el.parentElement.scrollHeight > el.parentElement.clientHeight + 10) {
                    el.parentElement.scrollTop = el.parentElement.scrollHeight;
                    return 'scrolled_parent';
                }
            }
            // Fallback: scroll the window
            window.scrollTo(0, document.body.scrollHeight);
            return 'scrolled_window';
        }""")
        await page.wait_for_timeout(1500)

    logger.info("Extracted %d data rows from table (after %d scroll attempts)",
                len(all_rows), min(attempt + 1, 100))
    return all_rows


async def _extract_summary_panel(page) -> dict:
    """Extract right panel summary cards from Market Finder."""
    summary = await page.evaluate("""() => {
        const data = {};

        // Extract from summary cards (label + value pairs)
        const cards = document.querySelectorAll(
            '[class*="Card"], [class*="card"], '
            + '[class*="Summary"], [class*="summary"], '
            + '[class*="Stat"], [class*="stat"], '
            + '[class*="Metric"], [class*="metric"]'
        );

        for (const card of cards) {
            const rect = card.getBoundingClientRect();
            // Only right panel cards (x > 600)
            if (rect.x < 500 || rect.width < 50) continue;

            const text = card.innerText.trim();
            const lines = text.split('\\n').map(l => l.trim()).filter(Boolean);
            if (lines.length >= 2) {
                const label = lines[0].toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
                const value = lines[1];
                if (label && value && label.length < 50) {
                    data[label] = value;
                }
            }
        }

        // Also try specific regex patterns on the page text
        const allText = document.body.innerText;
        const patterns = {
            'homeownership_rate': /Homeownership Rate[\\s:]*([\\d.]+%)/i,
            'market_rent': /Market Rent[\\s:]*\\$([\\d,.]+)/i,
            'gross_rental_yield': /Gross Rental Yield[\\s:]*([\\d.]+%)/i,
            'median_home_value_summary': /Median Home Value[\\s:]*\\$([\\d,.KkMm]+)/i,
            'homes_on_market_summary': /Homes on Market[\\s:]*([\\d,.KkMm]+)/i,
            'mo_investor_trans': /Mo\\.? Investor Transactions[\\s:]*([\\d,.]+)/i,
            'homes_sold_last_month': /Homes Sold Last Month[\\s:]*([\\d,.]+)/i,
        };

        for (const [key, pattern] of Object.entries(patterns)) {
            const match = allText.match(pattern);
            if (match) data[key] = match[1];
        }

        return data;
    }""")

    logger.info("Extracted summary panel: %d fields — %s", len(summary), list(summary.keys()))
    return summary


async def _switch_view(page, view_name: str) -> bool:
    """Toggle between ZIP Codes and Neighborhoods views.

    The view toggle is a styled-components Select dropdown (not a tab or button).
    It contains options: "ZIP Codes" and "Neighborhoods".
    From the DOM: class="Selectstyles__SelectContainer-..."

    IMPORTANT: Check current view first — if already on the requested view,
    don't click (clicking would toggle AWAY from it).
    """
    try:
        # Find the view toggle in the top bar (y < 150, x > 500)
        select_value = page.locator('[class*="SelectValue"]')
        for i in range(await select_value.count()):
            el = select_value.nth(i)
            bbox = await el.bounding_box()
            if bbox and bbox["y"] < 150 and bbox["x"] > 500:
                current_text = (await el.inner_text()).strip()
                logger.info("View toggle shows '%s', requested '%s'", current_text, view_name)

                # Already on the requested view — do nothing
                if view_name in current_text:
                    logger.info("Already on %s view — no switch needed", view_name)
                    return True

                # Need to switch — open dropdown and click the target option
                await el.click(force=True)
                await page.wait_for_timeout(1000)

                # Click the target option
                option = page.locator(f'[class*="SelectOption"]:has-text("{view_name}")')
                if await option.count() > 0:
                    await option.first.click(force=True)
                    await page.wait_for_timeout(3000)
                    logger.info("Switched to %s view", view_name)
                    return True

                # Fallback: text match
                option = page.get_by_text(view_name, exact=True)
                if await option.count() > 0:
                    await option.first.click(force=True)
                    await page.wait_for_timeout(3000)
                    logger.info("Switched to %s view via text click", view_name)
                    return True

                # Close dropdown without selecting
                await page.keyboard.press("Escape")
                logger.warning("Could not find '%s' option in dropdown", view_name)
                return False

        logger.warning("View toggle not found in top bar")
        return False
    except Exception as e:
        logger.error("Failed to switch to %s: %s", view_name, e)
        return False


# ── Main Extraction ───────────────────────────────────────────────────

async def extract_market_finder(
    state: str,
    county: str,
    *,
    email: str | None = None,
    password: str | None = None,
    headless: bool = False,
    output_dir: str = "./output",
) -> dict:
    """Extract Market Finder data for a state/county.

    Extracts ALL ZIP codes and ALL neighborhoods — scrolls through the
    entire table to ensure complete data. Never truncates.

    Returns:
        {
            "success": bool,
            "state": str,
            "county": str,
            "zip_data": [{"zip_code": "37920", ...}, ...],
            "neighborhood_data": [{"neighborhood": "Colonial Village", ...}, ...],
            "summary": {"median_home_value": "...", ...},
            "extracted_at": "2026-04-11T...",
            "zip_count": int,
            "neighborhood_count": int,
        }
    """
    result = {
        "success": False,
        "state": state,
        "county": county,
        "zip_data": [],
        "neighborhood_data": [],
        "summary": {},
        "extracted_at": datetime.now().isoformat(),
        "zip_count": 0,
        "neighborhood_count": 0,
    }

    async with create_browser(headless=headless) as (browser, context, page):
        # Step 1: Login
        logged_in = await login(page, email, password)
        if not logged_in:
            logger.error("Login failed")
            await screenshot(page, "market_finder_login_failed")
            return result

        # Step 2: Navigate to Market Finder
        if not await _navigate_to_market_finder(page):
            await screenshot(page, "market_finder_nav_failed")
            return result

        # Aggressively dismiss popups before any interaction
        await _dismiss_all_popups(page)
        await screenshot(page, "market_finder_loaded")

        # Step 3: Select state
        if not await _select_state(page, state):
            await screenshot(page, "market_finder_state_failed")
            return result

        await _dismiss_all_popups(page)
        await screenshot(page, f"market_finder_state_{state}")

        # Step 4: Select county
        if not await _select_county(page, county):
            await screenshot(page, "market_finder_county_failed")
            return result

        await _dismiss_all_popups(page)
        await screenshot(page, f"market_finder_county_{county}")

        # Step 5: Extract ZIP code data (default view)
        await _switch_view(page, "ZIP Codes")
        await page.wait_for_timeout(2000)

        zip_rows = await _extract_all_table_rows(page)
        for row in zip_rows:
            result["zip_data"].append(_row_to_dict(row, ZIP_COLUMNS))

        result["zip_count"] = len(result["zip_data"])
        logger.info("Extracted %d ZIP code records", result["zip_count"])
        await screenshot(page, f"market_finder_zips_{county}")

        # Step 6: Extract ALL neighborhood data
        if await _switch_view(page, "Neighborhoods"):
            await page.wait_for_timeout(2000)
            nbr_rows = await _extract_all_table_rows(page)
            for row in nbr_rows:
                result["neighborhood_data"].append(
                    _row_to_dict(row, NEIGHBORHOOD_COLUMNS)
                )

            result["neighborhood_count"] = len(result["neighborhood_data"])
            logger.info("Extracted %d neighborhood records", result["neighborhood_count"])
            await screenshot(page, f"market_finder_neighborhoods_{county}")

        # Step 7: Extract summary panel
        result["summary"] = await _extract_summary_panel(page)

        result["success"] = len(result["zip_data"]) > 0
        logger.info(
            "Market Finder extraction %s: %d ZIPs, %d neighborhoods",
            "succeeded" if result["success"] else "FAILED",
            result["zip_count"],
            result["neighborhood_count"],
        )

    # Save JSON output
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    filename = f"market_finder_{state}_{county}_{datetime.now():%Y%m%d_%H%M%S}.json"
    json_file = out_path / filename
    json_file.write_text(json.dumps(result, indent=2), encoding="utf-8")
    logger.info("Saved extraction to %s", json_file)

    return result


# ── CLI Entry Point ───────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extract Market Finder data from DataSift.ai"
    )
    parser.add_argument(
        "--state", required=True, help="State name (e.g., Tennessee)"
    )
    parser.add_argument(
        "--county",
        required=True,
        help="County name(s), comma-separated (e.g., Knox or Knox,Blount)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser in headless mode",
    )
    parser.add_argument(
        "--output-dir",
        default="./output",
        help="Output directory for JSON files (default: ./output)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose logging"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    counties = [c.strip() for c in args.county.split(",")]

    for county in counties:
        result = asyncio.run(
            extract_market_finder(
                state=args.state,
                county=county,
                headless=args.headless,
                output_dir=args.output_dir,
            )
        )
        if result["success"]:
            print(f"  {county}: {result['zip_count']} ZIP codes, "
                  f"{result['neighborhood_count']} neighborhoods")
        else:
            print(f"  {county}: EXTRACTION FAILED — check screenshots for debugging")
            sys.exit(1)


if __name__ == "__main__":
    main()
