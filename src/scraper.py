"""Core scraping logic — login, navigate saved searches, paginate results."""

import asyncio
import logging
import random
import re
from datetime import datetime, timedelta
from pathlib import Path

from playwright.async_api import Page, TimeoutError as PwTimeout, async_playwright

from captcha_solver import solve_captcha_and_view
import config
from config import (
    BASE_URL,
    COOKIES_FILE,
    LOGIN_URL,
    MAX_RETRIES,
    REQUEST_DELAY_MAX,
    REQUEST_DELAY_MIN,
    RESULTS_PER_PAGE,
    SAVED_SEARCHES,
    SEEN_IDS_FILE,
    SEEN_IDS_PRUNE_DAYS,
    SMART_SEARCH_URL,
    STATE_FILE,
    SavedSearch,
    SEL_LOGIN_EMAIL,
    SEL_LOGIN_PASSWORD,
    SEL_LOGIN_SUBMIT,
    SEL_NEXT_PAGE_BUTTON,
    SEL_PAGE_INFO,
    SEL_PER_PAGE_DROPDOWN,
    SEL_SAVED_SEARCHES_DROPDOWN,
    SEL_VIEW_BUTTON_PATTERN,
)
from data_formatter import _notice_id_from_url
from foreclosure_filter import is_valid_foreclosure
from notice_parser import NoticeData, is_target_county, parse_notice_page

logger = logging.getLogger(__name__)


async def delay() -> None:
    """Random delay between requests to avoid detection."""
    wait = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
    await asyncio.sleep(wait)


# ── Login ─────────────────────────────────────────────────────────────


async def login(page: Page, _retries: int = 3) -> bool:
    """Log in to tnpublicnotice.com Smart Search. Returns True on success.

    Retries up to ``_retries`` times on transient network errors (e.g. after
    Apify container migration).
    """
    for attempt in range(1, _retries + 1):
        try:
            logger.info("Logging in to %s (attempt %d/%d)", LOGIN_URL, attempt, _retries)
            await page.goto(LOGIN_URL)
            await page.wait_for_load_state("networkidle")
            break  # page loaded successfully
        except Exception as exc:
            logger.warning("Login navigation failed (attempt %d/%d): %s", attempt, _retries, exc)
            if attempt < _retries:
                await asyncio.sleep(5 * attempt)  # back off 5s, 10s
                continue
            logger.error("Login navigation failed after %d attempts — giving up", _retries)
            return False

    # No CAPTCHA on the login page (confirmed via research)
    await page.fill(SEL_LOGIN_EMAIL, config.TNPN_EMAIL)
    await page.fill(SEL_LOGIN_PASSWORD, config.TNPN_PASSWORD)
    await page.click(SEL_LOGIN_SUBMIT)
    await page.wait_for_load_state("networkidle")
    await delay()

    # Successful login redirects to /Smartsearch/Default.aspx
    if "smartsearch" in page.url.lower():
        logger.info("Login successful — on Smart Search dashboard")
        return True

    # Check for error message
    error = await page.query_selector(".error, .validation-summary-errors")
    if error:
        msg = await error.inner_text()
        logger.error("Login failed: %s", msg.strip())
    else:
        logger.error("Login failed — landed on %s", page.url)
    return False


# ── Saved Search Execution ────────────────────────────────────────────


def _get_session_base(page_url: str) -> str:
    """Extract the session-aware base URL from the current page URL.

    ASP.NET embeds session IDs in URL paths: /(S({guid}))/
    Returns the base URL including the session path segment.
    """
    m = re.search(r"(https?://[^/]+/\(S\([^)]+\)\)/)", page_url)
    if m:
        return m.group(1)
    return BASE_URL + "/"


async def _navigate_to_dashboard(page: Page) -> bool:
    """Ensure we're on the Smart Search dashboard.

    Returns True on success, False if session is dead and re-login is needed.
    """
    if "smartsearch/default" not in page.url.lower():
        session_base = _get_session_base(page.url)
        dashboard_url = session_base + "Smartsearch/Default.aspx"
        logger.info("Navigating to Smart Search dashboard: %s", dashboard_url)
        try:
            await page.goto(dashboard_url, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except PwTimeout:
            logger.warning("Dashboard navigation timed out")
            return False
        except Exception:
            logger.warning("Dashboard navigation failed", exc_info=True)
            return False
        await delay()

    # Session expired → ASP.NET redirected to authenticate page
    if "authenticate" in page.url.lower():
        logger.warning("Session expired — redirected to login page")
        return False

    dropdown = await page.query_selector(SEL_SAVED_SEARCHES_DROPDOWN)
    if not dropdown:
        logger.error("Saved Searches dropdown not found on dashboard")
        return False
    return True


async def _set_per_page(page: Page) -> None:
    """Set the results-per-page dropdown to max (50) if present."""
    dropdown = await page.query_selector(SEL_PER_PAGE_DROPDOWN)
    if dropdown:
        current = await dropdown.input_value()
        if current != str(RESULTS_PER_PAGE):
            logger.info("Setting results per page to %d", RESULTS_PER_PAGE)
            await page.select_option(SEL_PER_PAGE_DROPDOWN, str(RESULTS_PER_PAGE))
            await page.wait_for_load_state("networkidle")
            await delay()
            await delay()  # extra wait — ASP.NET DOM rebuild after postback


async def _get_page_info(page: Page) -> tuple[int, int]:
    """Parse 'Page X of Y Pages' text. Returns (current_page, total_pages)."""
    try:
        info_el = await page.query_selector(SEL_PAGE_INFO)
        if info_el:
            text = await info_el.inner_text()
            # "Page 1 of 100 Pages"
            import re
            m = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", text)
            if m:
                return int(m.group(1)), int(m.group(2))
    except Exception:
        pass
    return 1, 1


async def _extract_published_date(row_text: str) -> str:
    """Pull the 'Published: M/D/YYYY' date from a result row's text."""
    import re
    m = re.search(r"Published:\s*(\d{1,2}/\d{1,2}/\d{4})", row_text)
    if m:
        try:
            return datetime.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return m.group(1)
    return ""


async def run_saved_search(
    page: Page,
    search: SavedSearch,
    since_date: str | None = None,
    llm_api_key: str | None = None,
    on_page_batch=None,
    start_page: int = 1,
    max_notices: int = 0,
    seen_ids: dict[str, str] | None = None,
) -> list[NoticeData]:
    """Select a saved search from the dropdown, paginate, and scrape each notice.

    Args:
        on_page_batch: Optional async callback(list[NoticeData]) called after each page
                       to push results incrementally.
        start_page: Page number to start scraping from (default 1). Use this to
                    resume a previous run without re-scraping earlier pages.

    Returns list of parsed and filtered NoticeData.
    """
    logger.info("Running saved search: %s", search.saved_search_name)

    # Navigate to dashboard and select the saved search from dropdown
    if not await _navigate_to_dashboard(page):
        # Try re-login once and retry
        if await _try_relogin(page) and await _navigate_to_dashboard(page):
            pass  # recovered — continue below
        else:
            return []

    # Selecting from the dropdown triggers an ASP.NET postback → full page navigation.
    # Must wait for navigation explicitly or the execution context gets destroyed.
    try:
        async with page.expect_navigation(wait_until="networkidle", timeout=30000):
            await page.select_option(
                SEL_SAVED_SEARCHES_DROPDOWN,
                label=search.saved_search_name,
            )
    except Exception:
        logger.error("Could not select '%s' from dropdown", search.saved_search_name)
        return []

    await delay()

    # Verify we're on search results
    if "search" not in page.url.lower():
        logger.error("Expected Search.aspx but got %s", page.url)
        return []

    # Maximize results per page
    await _set_per_page(page)

    # Scrape all pages
    notices: list[NoticeData] = []
    current_page, total_pages = await _get_page_info(page)
    logger.info("  %d pages of results for %s", total_pages, search.saved_search_name)

    # Skip ahead to start_page if needed
    if start_page > 1:
        logger.info("  Skipping to page %d (start_page)", start_page)
        while current_page < start_page:
            next_btn = await page.query_selector(SEL_NEXT_PAGE_BUTTON)
            if not next_btn:
                logger.error("  Cannot reach page %d — no next button at page %d", start_page, current_page)
                return []
            await next_btn.click()
            await page.wait_for_load_state("load")
            await delay()
            current_page, total_pages = await _get_page_info(page)
        logger.info("  Reached page %d/%d", current_page, total_pages)

    while True:
        logger.info("  Scraping page %d/%d", current_page, total_pages)
        page_notices = await _scrape_results_page(page, search, since_date, llm_api_key, seen_ids)
        notices.extend(page_notices)

        # Push this page's results immediately so they survive timeouts
        if on_page_batch and page_notices:
            await on_page_batch(page_notices)

        # Stop early if we've hit the max_notices limit
        if max_notices and len(notices) >= max_notices:
            logger.info("  Reached max_notices limit (%d) — stopping", max_notices)
            notices = notices[:max_notices]
            break

        # Check if there's a next page
        if current_page >= total_pages:
            break

        next_btn = await page.query_selector(SEL_NEXT_PAGE_BUTTON)
        can_advance = next_btn and not await next_btn.get_attribute("disabled") if next_btn else False

        if can_advance:
            await next_btn.click()
            await page.wait_for_load_state("load")
            await delay()
            await delay()
            current_page, total_pages = await _get_page_info(page)
        else:
            # Grid lost or next button missing — attempt recovery to next page
            if current_page < total_pages:
                logger.warning(
                    "  Grid lost on page %d/%d — attempting recovery",
                    current_page, total_pages,
                )
                recovered = await _recover_to_search_page(
                    page, search, current_page + 1,
                )
                if recovered:
                    current_page, total_pages = await _get_page_info(page)
                    continue
                logger.error("  Recovery failed — stopping after page %d", current_page)
            break

    logger.info("  Found %d notices for %s", len(notices), search.saved_search_name)
    return notices


# ── Per-Page Scraping ─────────────────────────────────────────────────


async def _scrape_results_page(
    page: Page,
    search: SavedSearch,
    since_date: str | None,
    llm_api_key: str | None = None,
    seen_ids: dict[str, str] | None = None,
) -> list[NoticeData]:
    """Click each View button on a results page, solve CAPTCHA, parse notice."""
    notices: list[NoticeData] = []

    # Wait for view buttons to be stable in the DOM before interacting
    try:
        await page.wait_for_selector(SEL_VIEW_BUTTON_PATTERN, state="attached", timeout=10_000)
    except PwTimeout:
        logger.warning("  No view buttons appeared within 10s")
        return notices

    # Find all View buttons in the results grid
    view_buttons = await page.query_selector_all(SEL_VIEW_BUTTON_PATTERN)
    num_results = len(view_buttons)
    logger.info("  %d results on this page", num_results)

    if num_results == 0:
        return notices

    # We need to iterate by index because clicking a view button navigates away.
    # After parsing each notice, we navigate back and re-find the buttons.
    grid_lost = False
    for idx in range(num_results):
        if grid_lost:
            break
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Re-find all view buttons (DOM refreshes after back-navigation)
                view_buttons = await page.query_selector_all(SEL_VIEW_BUTTON_PATTERN)
                if idx >= len(view_buttons):
                    logger.warning("  Button index %d out of range (%d buttons)", idx, len(view_buttons))
                    if len(view_buttons) == 0:
                        logger.warning("  Results grid lost — stopping this page")
                        grid_lost = True
                    break

                # Grab the row text for date and preview before navigating
                btn = view_buttons[idx]
                row = await btn.evaluate_handle("el => el.closest('tr').parentElement.closest('tr')")
                row_text = ""
                try:
                    row_text = await row.evaluate("el => el.innerText")
                except Exception:
                    pass

                # Check published date for daily mode cutoff
                pub_date = await _extract_published_date(row_text)
                if since_date and pub_date and pub_date < since_date:
                    logger.debug("  Skipping old notice (%s < %s)", pub_date, since_date)
                    break

                # Click the View button → navigates to Details.aspx
                await btn.click()
                await page.wait_for_load_state("networkidle")
                await delay()

                # Cross-run dedup: if we've seen this notice ID before, skip CAPTCHA entirely
                notice_id = _notice_id_from_url(page.url)
                if seen_ids is not None and notice_id and notice_id in seen_ids:
                    logger.info("  Skipping already-processed notice ID=%s", notice_id)
                    await page.go_back()
                    await page.wait_for_load_state("networkidle")
                    await delay()
                    break  # next result

                # Check if notice content is already visible (CAPTCHA previously solved in session)
                content_visible = await page.query_selector("text='Notice Content'")
                if not content_visible:
                    # Need to solve CAPTCHA
                    if not await solve_captcha_and_view(page):
                        logger.warning("  CAPTCHA solve failed for result %d (attempt %d)", idx + 1, attempt)
                        # Navigate back and retry
                        await page.go_back()
                        await page.wait_for_load_state("networkidle")
                        await delay()
                        continue

                # Parse the now-visible notice text
                notice = await parse_notice_page(page, search.county, search.notice_type, llm_api_key)
                if pub_date:
                    notice.date_added = pub_date

                # Record this notice ID so future runs don't re-process it
                if seen_ids is not None and notice_id:
                    seen_ids[notice_id] = notice.date_added or datetime.now().strftime("%Y-%m-%d")

                # Apply foreclosure filter
                if not is_valid_foreclosure(notice):
                    logger.debug("  Filtered out (not foreclosure): %s", notice.source_url)
                # Apply county validation — reject notices where the property
                # is actually in a different county (search false positive)
                elif not is_target_county(notice.raw_text, search.county):
                    logger.debug("  Filtered out (wrong county): %s", notice.source_url)
                else:
                    notices.append(notice)
                    logger.debug("  Kept notice: %s", notice.source_url)

                # Navigate back to the results page
                await page.go_back()
                await page.wait_for_load_state("networkidle")
                # Sometimes the back takes us to the CAPTCHA page, need another back
                if "details" in page.url.lower():
                    await page.go_back()
                    await page.wait_for_load_state("networkidle")
                await delay()
                break  # Success — next result

            except PwTimeout:
                logger.warning("  Timeout on result %d (attempt %d/%d)", idx + 1, attempt, MAX_RETRIES)
                # Try to recover by going back to results
                try:
                    await page.go_back()
                    await page.wait_for_load_state("networkidle")
                except Exception:
                    pass
                await delay()

            except Exception:
                logger.exception("  Error on result %d (attempt %d/%d)", idx + 1, attempt, MAX_RETRIES)
                # Only go back if we actually navigated away from search results
                if "search" not in page.url.lower():
                    try:
                        await page.go_back()
                        await page.wait_for_load_state("networkidle")
                    except Exception:
                        pass
                await delay()

    return notices


# ── Session Persistence ───────────────────────────────────────────────


async def _save_cookies(context) -> None:
    """Save browser cookies to disk for session reuse."""
    try:
        cookies = await context.cookies()
        config.save_state(COOKIES_FILE, cookies)
        logger.debug("Saved %d cookies to %s", len(cookies), COOKIES_FILE)
    except Exception:
        logger.debug("Could not save cookies", exc_info=True)


async def _load_cookies(context) -> bool:
    """Load saved cookies into browser context. Returns True if loaded."""
    cookies = config.load_state(COOKIES_FILE)
    if not cookies:
        return False
    try:
        await context.add_cookies(cookies)
        logger.debug("Loaded %d cookies from %s", len(cookies), COOKIES_FILE)
        return True
    except Exception:
        logger.debug("Could not load cookies", exc_info=True)
        return False


async def _try_relogin(page: Page) -> bool:
    """Detect if session expired and attempt re-login. Returns True if re-login succeeded."""
    # Check if we're on the authenticate page or if dashboard nav fails
    is_dead = "authenticate" in page.url.lower()
    if not is_dead:
        # Quick check: try navigating to dashboard
        try:
            await page.goto(SMART_SEARCH_URL, wait_until="domcontentloaded", timeout=15_000)
            await page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            is_dead = True
        else:
            is_dead = "authenticate" in page.url.lower()

    if not is_dead:
        return False  # Session is fine, failure was something else

    logger.warning("Session expired — attempting re-login")
    if await login(page):
        logger.info("Re-login successful")
        return True

    logger.error("Re-login failed")
    return False


async def _recover_to_search_page(
    page: Page, search: SavedSearch, target_page: int,
) -> bool:
    """Recover from a lost results grid by re-logging in and navigating to target_page."""
    logger.warning("Attempting to recover search session (target page %d)", target_page)

    # Re-login if session expired
    if "authenticate" in page.url.lower() or not await _navigate_to_dashboard(page):
        if not await _try_relogin(page):
            logger.error("Cannot re-login — recovery failed")
            return False
        if not await _navigate_to_dashboard(page):
            return False

    # Re-select the saved search
    try:
        async with page.expect_navigation(wait_until="networkidle", timeout=30000):
            await page.select_option(
                SEL_SAVED_SEARCHES_DROPDOWN,
                label=search.saved_search_name,
            )
    except Exception:
        logger.error("Could not re-select '%s' during recovery", search.saved_search_name)
        return False

    await delay()

    if "search" not in page.url.lower():
        return False

    await _set_per_page(page)

    # Navigate to target page by clicking "Next page" repeatedly
    current, total = await _get_page_info(page)
    while current < target_page:
        try:
            next_btn = await page.query_selector(SEL_NEXT_PAGE_BUTTON)
            if not next_btn:
                logger.error("Next page button not found during recovery at page %d", current)
                return False
            await next_btn.click()
            await page.wait_for_load_state("load")
            await delay()
            await delay()
            current, total = await _get_page_info(page)
        except Exception:
            logger.warning("Recovery navigation failed at page %d", current, exc_info=True)
            return False

    logger.info("Recovery successful — now on page %d/%d", current, total)
    return True


async def _is_session_valid(page: Page) -> bool:
    """Check if saved cookies give us a valid logged-in session."""
    try:
        await page.goto(SMART_SEARCH_URL)
        await page.wait_for_load_state("networkidle")
        # If we land on the dashboard, session is valid
        if "smartsearch" in page.url.lower():
            dropdown = await page.query_selector(SEL_SAVED_SEARCHES_DROPDOWN)
            if dropdown:
                logger.info("Reusing saved session — already logged in")
                return True
    except Exception:
        pass
    return False


# ── State Tracking ────────────────────────────────────────────────────


def load_last_run_date() -> str | None:
    """Load the date of the last successful run from state file."""
    data = config.load_state(STATE_FILE)
    return data.get("last_run_date")


def save_last_run_date() -> None:
    """Save today's date as the last run date."""
    config.save_state(STATE_FILE, {"last_run_date": datetime.now().strftime("%Y-%m-%d")})


def load_seen_ids() -> dict[str, str]:
    """Load notice IDs already processed in prior runs, pruning entries older than SEEN_IDS_PRUNE_DAYS.

    Returns a dict of {notice_id: "YYYY-MM-DD"}. The date is when we first saw the
    notice, used only for pruning to bound file size.
    """
    data = config.load_state(SEEN_IDS_FILE)
    if not data:
        return {}
    cutoff = (datetime.now() - timedelta(days=SEEN_IDS_PRUNE_DAYS)).strftime("%Y-%m-%d")
    pruned = {nid: d for nid, d in data.items() if d >= cutoff}
    if len(pruned) < len(data):
        logger.info("Pruned %d seen IDs older than %d days", len(data) - len(pruned), SEEN_IDS_PRUNE_DAYS)
    return pruned


def save_seen_ids(seen: dict[str, str]) -> None:
    """Persist the seen-notice-ID cache to disk."""
    config.save_state(SEEN_IDS_FILE, seen)


# ── Main Entry Point ─────────────────────────────────────────────────


async def scrape_all(
    mode: str = "daily",
    searches: list[SavedSearch] | None = None,
    proxy_url: str | None = None,
    on_batch=None,
    since_date_override: str | None = None,
    llm_api_key: str | None = None,
    start_page: int = 1,
    max_notices: int = 0,
    seen_ids: dict[str, str] | None = None,
    on_search_complete=None,
) -> list[NoticeData]:
    """Main entry point for scraping.

    Args:
        mode: "daily" (only new since last run) or "historical" (last 12 months).
        searches: Optional subset of searches to run. Defaults to all.
        proxy_url: Optional proxy URL (e.g. Apify residential proxy).
        on_batch: Optional async callback(list[NoticeData]) called after each search.
        since_date_override: If set (YYYY-MM-DD), overrides the mode-based date logic.
        start_page: Start scraping from this page number (default 1).
        seen_ids: Cross-run dict of already-processed notice IDs. If None, loads from
                  SEEN_IDS_FILE. Caller (e.g. Apify) can pass its own dict loaded
                  from KVS to participate in the dedup cache.
        on_search_complete: Optional async callback(seen_ids) fired after each search
                            completes, so callers can persist seen_ids to their own
                            backing store (e.g. Apify KVS).

    Returns:
        All scraped and filtered NoticeData.
    """
    if searches is None:
        searches = SAVED_SEARCHES

    # Load the cross-run seen-ID cache (caller may have pre-loaded for KVS-backed stores)
    if seen_ids is None:
        seen_ids = load_seen_ids()
    logger.info("Cross-run dedup: %d previously-seen notice IDs loaded", len(seen_ids))

    # Determine date cutoff
    since_date: str | None = None
    if since_date_override:
        since_date = since_date_override
        logger.info("Using since_date override: %s", since_date)
    elif mode == "daily":
        since_date = load_last_run_date()
        if since_date:
            logger.info("Daily mode: pulling notices since %s", since_date)
        else:
            logger.info("Daily mode: no previous run found, pulling last 7 days")
            since_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    elif mode == "historical":
        since_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        logger.info("Historical mode: pulling notices since %s", since_date)

    all_notices: list[NoticeData] = []

    async with async_playwright() as p:
        launch_opts: dict = {"headless": True}
        if proxy_url:
            # Parse proxy URL (format: http://user:pass@host:port)
            from urllib.parse import urlparse
            parsed = urlparse(proxy_url)
            proxy_cfg: dict = {
                "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}",
            }
            if parsed.username:
                proxy_cfg["username"] = parsed.username
            if parsed.password:
                proxy_cfg["password"] = parsed.password
            launch_opts["proxy"] = proxy_cfg
            logger.info("Using proxy: %s:%s", parsed.hostname, parsed.port)

        browser = await p.chromium.launch(**launch_opts)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        # Generous timeout for ASP.NET postbacks + CAPTCHA solving
        context.set_default_timeout(60_000)

        # Try to reuse saved session cookies
        await _load_cookies(context)
        page = await context.new_page()

        if not await _is_session_valid(page):
            # Fresh login required
            if not await login(page):
                logger.error("Login failed — aborting scrape")
                await browser.close()
                return []
            # Save cookies for next run
            await _save_cookies(context)

        for search in searches:
            # Proactive session check — re-login if session died between searches
            if "authenticate" in page.url.lower():
                if not await _try_relogin(page):
                    logger.error("Cannot recover session — aborting remaining searches")
                    break

            remaining = (max_notices - len(all_notices)) if max_notices else 0
            try:
                search_notices = await run_saved_search(
                    page, search, since_date, llm_api_key,
                    on_page_batch=on_batch, start_page=start_page,
                    max_notices=remaining, seen_ids=seen_ids,
                )
                all_notices.extend(search_notices)
            except Exception:
                logger.exception("Failed to scrape: %s", search.saved_search_name)
                # Check if failure was due to session expiration and re-login
                if await _try_relogin(page):
                    try:
                        search_notices = await run_saved_search(
                            page, search, since_date, llm_api_key,
                            on_page_batch=on_batch, start_page=start_page,
                            max_notices=remaining, seen_ids=seen_ids,
                        )
                        all_notices.extend(search_notices)
                    except Exception:
                        logger.exception("Still failing after re-login: %s", search.saved_search_name)

            # Incremental persistence — if a later search crashes fatally, progress
            # from completed searches is not lost. Covers the re-pull bug where a
            # single end-of-run save at line 722 used to silently skip on exceptions.
            try:
                save_seen_ids(seen_ids)
                if mode == "daily":
                    save_last_run_date()
                if on_search_complete is not None:
                    await on_search_complete(seen_ids)
            except Exception:
                logger.exception("Failed to persist seen_ids after %s", search.saved_search_name)

            if max_notices and len(all_notices) >= max_notices:
                logger.info("Reached max_notices limit (%d) — stopping", max_notices)
                break

        await browser.close()

    if mode == "daily":
        save_last_run_date()
    save_seen_ids(seen_ids)

    logger.info("Total notices scraped: %d", len(all_notices))
    return all_notices
