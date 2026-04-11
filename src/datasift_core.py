"""DataSift.ai shared automation primitives — login, cookies, UI helpers.

Self-contained module for use in both the SiftStack pipeline and
distributed .skill packages. Only requires playwright + python-dotenv.

When used inside SiftStack (src/), it loads credentials from config.py.
When used standalone in a skill ZIP, it loads from .env or environment vars.
"""

__version__ = "1.0.0"

import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

# ── URLs ──────────────────────────────────────────────────────────────
DATASIFT_LOGIN_URL = "https://app.reisift.io/login"
DATASIFT_DASHBOARD_URL = "https://app.reisift.io/dashboard/general"
DATASIFT_RECORDS_URL = "https://app.reisift.io/records/properties"
DATASIFT_SIFTMAP_URL = "https://app.reisift.io/siftmap"
DATASIFT_MARKET_FINDER_URL = "https://app.reisift.io/market-finder"

# ── Browser Defaults ──────────────────────────────────────────────────
DEFAULT_VIEWPORT = {"width": 1440, "height": 900}
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


# ── Capability Detection ─────────────────────────────────────────────

def has_playwright() -> bool:
    """Check if Playwright is available in this environment."""
    try:
        import playwright  # noqa: F401
        return True
    except ImportError:
        return False


def detect_context() -> str:
    """Detect execution context: 'claude_code', 'co_work', or 'standalone'."""
    if os.getenv("CLAUDE_CODE"):
        return "claude_code"
    if not has_playwright():
        return "co_work"
    return "standalone"


# ── Credentials ───────────────────────────────────────────────────────

def get_credentials() -> tuple[str, str]:
    """Get DataSift email and password from environment or .env file.

    Returns (email, password). Raises ValueError if not found.
    """
    # Try loading from .env (works both in SiftStack and standalone)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # dotenv not required if env vars are set directly

    email = os.getenv("DATASIFT_EMAIL", "")
    password = os.getenv("DATASIFT_PASSWORD", "")

    if not email or not password:
        raise ValueError(
            "DATASIFT_EMAIL and DATASIFT_PASSWORD must be set in .env or environment"
        )
    return email, password


# ── Cookie / State Persistence ────────────────────────────────────────

def save_state(path: Path, data) -> None:
    """Write JSON state to disk with .bak backup."""
    if path.exists():
        try:
            bak = path.with_suffix(path.suffix + ".bak")
            bak.write_bytes(path.read_bytes())
        except OSError:
            pass
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_state(path: Path):
    """Load JSON state from disk, falling back to .bak if corrupt."""
    for candidate in [path, path.with_suffix(path.suffix + ".bak")]:
        if candidate.exists():
            try:
                return json.loads(candidate.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to read %s: %s", candidate, e)
    return {}


# ── Cookie Management ─────────────────────────────────────────────────

COOKIES_FILE = Path("datasift_cookies.json")


async def save_cookies(page) -> None:
    """Save browser cookies for session reuse."""
    cookies = await page.context.cookies()
    save_state(COOKIES_FILE, cookies)
    logger.debug("Saved %d DataSift cookies", len(cookies))


async def load_cookies(context) -> bool:
    """Load saved cookies into browser context. Returns True if loaded."""
    cookies = load_state(COOKIES_FILE)
    if not cookies:
        return False
    try:
        await context.add_cookies(cookies)
        logger.debug("Loaded %d DataSift cookies", len(cookies))
        return True
    except Exception as e:
        logger.debug("Failed to load cookies: %s", e)
        return False


# ── Authentication ────────────────────────────────────────────────────

async def login(page, email: str = None, password: str = None) -> bool:
    """Log in to DataSift.ai (app.reisift.io). Returns True on success.

    Tries saved cookies first, falls back to fresh login.
    If email/password not provided, loads from environment.
    """
    from playwright.async_api import TimeoutError as PwTimeout

    if not email or not password:
        email, password = get_credentials()

    # Try cookies first
    has_cookies = await load_cookies(page.context)
    if has_cookies:
        await page.goto(DATASIFT_RECORDS_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)
        current_url = page.url
        if "/login" not in current_url and ("/dashboard" in current_url or "/records" in current_url):
            logger.info("DataSift session restored from cookies")
            return True
        logger.info("DataSift cookies expired (url=%s), doing fresh login", current_url)

    # Fresh login
    await page.goto(DATASIFT_LOGIN_URL, wait_until="domcontentloaded")

    # Fill credentials
    await page.get_by_role("textbox", name="Email").fill(email)
    await page.get_by_role("textbox", name="Password").fill(password)

    # Hidden checkboxes — click labels, not inputs
    remember_label = page.locator('label:has-text("Remember me")')
    if await remember_label.count() > 0:
        await remember_label.first.click()

    terms_label = page.locator('label:has-text("I\'ve read and agree")')
    if await terms_label.count() > 0:
        await terms_label.first.click()

    # Click Sign In
    await page.get_by_role("button", name="Sign In").click()

    # Wait for navigation away from login page
    try:
        await page.wait_for_url("**/dashboard/general**", timeout=15000)
    except PwTimeout:
        if "/login" in page.url:
            logger.error("DataSift login failed — still on login page")
            return False

    await save_cookies(page)
    logger.info("DataSift login successful")
    return True


# ── UI Primitives ─────────────────────────────────────────────────────

async def screenshot(page, name: str) -> None:
    """Take a debug screenshot (saved to working directory)."""
    try:
        await page.screenshot(path=f"datasift_{name}.png")
        logger.debug("Screenshot: datasift_%s.png", name)
    except Exception as e:
        logger.debug("Screenshot failed (%s): %s", name, e)


async def dismiss_popups(page) -> None:
    """Dismiss notification popups and remove Beamer NPS overlay.

    The Beamer NPS survey iframe (#npsIframeContainer) blocks ALL pointer
    events globally — it MUST be removed before any click interactions.
    """
    try:
        # Try clicking dismiss text elements first
        for text in ["NO, THANKS", "No, thanks", "No Thanks", "NO THANKS", "Not Now", "Dismiss"]:
            el = page.get_by_text(text, exact=True)
            if await el.count() > 0:
                await el.first.click(force=True)
                await page.wait_for_timeout(1000)
                logger.debug("Dismissed popup via '%s'", text)
                return

        # JavaScript fallback: remove popup elements from DOM
        removed = await page.evaluate("""() => {
            let removed = 0;
            // Remove Beamer NPS survey iframe (blocks pointer events globally)
            const nps = document.getElementById('npsIframeContainer');
            if (nps) { nps.remove(); removed++; }
            // Also remove by class
            document.querySelectorAll('[class*="nps-iframe"], [class*="beamer"]').forEach(
                el => { el.remove(); removed++; }
            );
            // Look for the notification popup overlay
            const els = document.querySelectorAll(
                '[class*="notification"], [class*="Notification"], '
                + '[class*="popup"], [class*="Popup"]'
            );
            for (const el of els) {
                if (el.textContent && el.textContent.includes('notifications')) {
                    el.remove();
                    removed++;
                }
            }
            // Also try removing any fixed/absolute overlays
            const overlays = document.querySelectorAll(
                '[style*="position: fixed"], [style*="position:fixed"]'
            );
            for (const o of overlays) {
                if (o.textContent && o.textContent.includes('notifications')) {
                    o.remove();
                    removed++;
                }
            }
            return removed;
        }""")
        if removed:
            logger.debug("Removed %d popup elements via JS", removed)
            await page.wait_for_timeout(500)
    except Exception as e:
        logger.debug("Popup dismissal failed: %s", e)


async def scroll_into_view(page, element) -> None:
    """Scroll an element into view using JS (Playwright scroll fails on DataSift panels).

    DataSift filter panels are scrollable <div>s, NOT the viewport.
    Playwright's scroll_into_view_if_needed() does nothing for these.
    """
    await page.evaluate(
        "el => el.scrollIntoView({behavior: 'instant', block: 'center'})",
        element,
    )
    await page.wait_for_timeout(300)


async def click_styled_dropdown(page, container_selector: str, option_text: str) -> bool:
    """Click a styled-components dropdown and select an option by text.

    DataSift has NO native <select> elements — all dropdowns are
    [class*="Selectstyles__Select"] containers with custom option elements.

    Args:
        page: Playwright page
        container_selector: CSS selector for the dropdown container
        option_text: Text of the option to select

    Returns:
        True if option was selected, False otherwise
    """
    try:
        # Click the dropdown to open it
        dropdown = page.locator(container_selector).first
        await dropdown.click()
        await page.wait_for_timeout(500)

        # Find and click the option
        option = page.locator(f'[class*="SelectOption"]:has-text("{option_text}")').first
        await option.wait_for(state="visible", timeout=5000)
        await option.click()
        await page.wait_for_timeout(500)
        return True
    except Exception as e:
        logger.warning("Failed to select '%s' from dropdown: %s", option_text, e)
        return False


async def wait_for_spa(page, ms: int = 5000) -> None:
    """Wait for DataSift SPA to settle after navigation.

    Use wait_until='domcontentloaded' (NOT 'networkidle') because
    the SPA keeps WebSocket connections open permanently.
    """
    await page.wait_for_timeout(ms)


async def extract_table_data(page, table_selector: str = "table") -> list[list[str]]:
    """Extract all rows from a table or table-like element via JS.

    Handles both standard <table> elements and styled-components tables.
    Returns a list of rows, where each row is a list of cell text values.
    """
    data = await page.evaluate(f"""() => {{
        const rows = [];
        const table = document.querySelector('{table_selector}') ||
                      document.querySelector('[class*="Table"]') ||
                      document.querySelector('[role="table"]');
        if (!table) return rows;

        // Try standard table rows first
        let trs = table.querySelectorAll('tr');
        if (trs.length > 0) {{
            for (const tr of trs) {{
                const cells = tr.querySelectorAll('td, th');
                const row = Array.from(cells).map(c => c.innerText.trim());
                if (row.length > 0) rows.push(row);
            }}
            return rows;
        }}

        // Fallback: div-based table (styled-components)
        const divRows = table.querySelectorAll('[class*="Row"], [class*="row"]');
        for (const dr of divRows) {{
            const cells = dr.querySelectorAll('[class*="Cell"], [class*="cell"], div > span');
            const row = Array.from(cells).map(c => c.innerText.trim());
            if (row.length > 0) rows.push(row);
        }}
        return rows;
    }}""")
    return data


async def scroll_and_extract_all(page, table_selector: str = "table",
                                  scroll_container: str = None,
                                  max_scrolls: int = 50) -> list[list[str]]:
    """Scroll through a lazy-loaded table and extract all rows.

    Handles DataSift's infinite scroll / lazy loading by scrolling the
    container, waiting for new rows, and re-extracting until stable.

    Args:
        page: Playwright page
        table_selector: CSS selector for the table element
        scroll_container: CSS selector for the scrollable container (if different from table)
        max_scrolls: Maximum number of scroll attempts

    Returns:
        All unique rows from the table
    """
    all_rows = []
    seen_keys = set()
    prev_count = 0

    for i in range(max_scrolls):
        data = await extract_table_data(page, table_selector)

        for row in data:
            key = "|".join(row)
            if key not in seen_keys:
                seen_keys.add(key)
                all_rows.append(row)

        if len(all_rows) == prev_count and i > 0:
            logger.debug("No new rows after scroll %d (total: %d)", i, len(all_rows))
            break

        prev_count = len(all_rows)

        # Scroll the container down
        container = scroll_container or table_selector
        await page.evaluate(f"""() => {{
            const el = document.querySelector('{container}') ||
                       document.querySelector('[class*="Table"]') ||
                       document.querySelector('[role="table"]');
            if (el) el.scrollTop = el.scrollHeight;
        }}""")
        await page.wait_for_timeout(1500)

    logger.info("Extracted %d total rows from table", len(all_rows))
    return all_rows


# ── Browser Lifecycle ─────────────────────────────────────────────────

@asynccontextmanager
async def create_browser(headless: bool = False, viewport: dict = None):
    """Create a Playwright browser context. Yields (browser, context, page).

    Usage:
        async with create_browser(headless=False) as (browser, context, page):
            await login(page)
            # ... do work ...
    """
    from playwright.async_api import async_playwright

    vp = viewport or DEFAULT_VIEWPORT

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport=vp,
            user_agent=DEFAULT_USER_AGENT,
        )
        page = await context.new_page()
        try:
            yield browser, context, page
        finally:
            await browser.close()
