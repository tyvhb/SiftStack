"""Upload CSV files to DataSift.ai (REISift) via Playwright browser automation.

DataSift has no public REST API, so we automate the web UI:
1. Upload wizard: "Upload File" → Add data → organize → tags → upload → map → finish
2. Enrich: Manage → Enrich Data → Enrich Property Information (SiftMap)
3. Skip Trace: Send To → Skip Trace → agree terms → process (unlimited plan)

Requires: DATASIFT_EMAIL and DATASIFT_PASSWORD in .env or environment.
"""

import logging
import os
from pathlib import Path

import config
from playwright.async_api import Page, TimeoutError as PwTimeout, async_playwright

logger = logging.getLogger(__name__)

DATASIFT_LOGIN_URL = "https://app.reisift.io/login"
DATASIFT_UPLOAD_URL = "https://app.reisift.io/records/properties"
DATASIFT_COOKIES_FILE = Path("datasift_cookies.json")


async def _save_cookies(page: Page) -> None:
    """Save browser cookies for session reuse."""
    cookies = await page.context.cookies()
    config.save_state(DATASIFT_COOKIES_FILE, cookies)
    logger.debug("Saved %d DataSift cookies", len(cookies))


async def _load_cookies(context) -> bool:
    """Load saved cookies into browser context. Returns True if loaded."""
    cookies = config.load_state(DATASIFT_COOKIES_FILE)
    if not cookies:
        return False
    try:
        await context.add_cookies(cookies)
        logger.debug("Loaded %d DataSift cookies", len(cookies))
        return True
    except Exception as e:
        logger.debug("Failed to load cookies: %s", e)
        return False


async def login(page: Page, email: str, password: str) -> bool:
    """Log in to DataSift.ai (app.reisift.io). Returns True on success.

    Tries saved cookies first, falls back to fresh login.
    """
    # Try cookies first
    has_cookies = await _load_cookies(page.context)
    if has_cookies:
        await page.goto(DATASIFT_UPLOAD_URL, wait_until="domcontentloaded")
        # Wait for SPA to settle — React app may redirect to login
        await page.wait_for_timeout(5000)
        # Check if we ended up on the login page or root (which also redirects to login)
        current_url = page.url
        if "/login" not in current_url and "/dashboard" in current_url or "/records" in current_url:
            logger.info("DataSift session restored from cookies")
            return True
        logger.info("DataSift cookies expired (url=%s), doing fresh login", current_url)

    # Fresh login
    await page.goto(DATASIFT_LOGIN_URL, wait_until="domcontentloaded")

    # Fill credentials
    await page.get_by_role("textbox", name="Email").fill(email)
    await page.get_by_role("textbox", name="Password").fill(password)

    # Check "Remember me" and terms of use checkboxes
    # These checkboxes use hidden inputs with styled labels — click the label instead
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
        # May redirect somewhere else — check we're not still on login
        if "/login" in page.url:
            logger.error("DataSift login failed — still on login page")
            return False

    await _save_cookies(page)
    logger.info("DataSift login successful")
    return True


async def _screenshot(page: Page, name: str) -> None:
    """Take a debug screenshot (saved to project root)."""
    try:
        await page.screenshot(path=f"datasift_{name}.png")
        logger.debug("Screenshot: datasift_%s.png", name)
    except Exception as e:
        logger.debug("Screenshot failed (%s): %s", name, e)


async def _click_next_step(page: Page, timeout: int = 20000) -> bool:
    """Click the 'Next Step' button that appears in the upload wizard.

    Default timeout is 20s to handle slow SPA rendering in headless/cloud
    environments (Apify containers take longer than local desktop).
    """
    try:
        btn = page.locator(
            'button:has-text("Next Step"), '
            'button:has-text("Next"), '
            'button:has-text("Continue")'
        )
        await btn.first.wait_for(state="visible", timeout=timeout)
        await btn.first.click()
        await page.wait_for_timeout(2000)
        return True
    except PwTimeout:
        logger.warning("Next Step button not found within %dms", timeout)
        return False


async def upload_csv(
    page: Page,
    csv_path: Path,
    *,
    mode: str = "add",
    list_name: str | None = None,
    existing_list: bool = False,
) -> dict:
    """Upload a CSV file to DataSift via the 7-step upload wizard.

    Wizard steps:
    1. Click "Upload File" in sidebar
    2. Choose "Add data" or "Update data"
    3. "Let's Stay Organized" questions
    4. Tags step (CSV has Tags column — skip adding custom tags)
    5. Browse/upload CSV file
    6. Map columns (auto-maps recognized headers)
    7. Review and "Finish Upload"

    Args:
        page: Logged-in Playwright page.
        csv_path: Path to the DataSift-formatted CSV file.
        mode: "add" (create new + update existing) or "update" (update only).
        list_name: Target list name. Required when existing_list=True.
        existing_list: If True, select "Adding properties to an existing list"
            instead of creating a new list. The list must already exist in DataSift.

    Returns:
        Dict with upload results: {success, records_uploaded, errors, message}
    """
    result = {
        "success": False,
        "records_uploaded": 0,
        "errors": 0,
        "message": "",
    }

    if not csv_path.exists():
        result["message"] = f"CSV file not found: {csv_path}"
        logger.error(result["message"])
        return result

    # ── Step 1: Click "Upload File" in sidebar ──
    logger.info("Step 1: Clicking Upload File...")
    # Navigate to records page (skip if already there from login)
    if "/records" not in page.url:
        await page.goto(DATASIFT_UPLOAD_URL, wait_until="domcontentloaded")
    # Wait for SPA to fully render (longer for headless/cloud environments)
    await page.wait_for_timeout(8000)

    # Dismiss notifications popup if present
    try:
        no_thanks = page.locator('button:has-text("NO, THANKS"), button:has-text("No, thanks")')
        if await no_thanks.count() > 0:
            await no_thanks.first.click()
            await page.wait_for_timeout(500)
            logger.debug("Dismissed notifications popup")
    except Exception as e:
        logger.debug("Popup dismissal failed: %s", e)

    try:
        # The Upload File button is in the sidebar — it's a styled element, not a <button>
        upload_btn = page.locator('text="Upload File"')
        if await upload_btn.count() == 0:
            upload_btn = page.locator(
                'a:has-text("Upload File"), '
                'div:has-text("Upload File") >> visible=true, '
                'button:has-text("Upload File"), '
                '[data-testid="upload-file"]'
            )
        if await upload_btn.count() > 0:
            await upload_btn.first.click()
            await page.wait_for_timeout(3000)
        else:
            await _screenshot(page, "step1_no_upload_btn")
            result["message"] = "Could not find Upload File button"
            logger.error(result["message"])
            return result
    except Exception as e:
        result["message"] = f"Step 1 failed: {e}"
        logger.error(result["message"])
        return result

    await _screenshot(page, "step1_wizard_opened")

    # Dismiss notifications popup if it appeared over the wizard
    try:
        no_thanks = page.locator('button:has-text("NO, THANKS")')
        if await no_thanks.count() > 0:
            await no_thanks.first.click()
            await page.wait_for_timeout(1000)
            logger.debug("Dismissed notifications popup")
    except Exception as e:
        logger.debug("Popup dismissal failed: %s", e)

    # ── Wizard Step 1: Setup ──
    # Part A: Select "Add Data" or "Update Data"
    logger.info("Wizard Step 1: Selecting '%s' mode...", mode)
    try:
        mode_label = "Add Data" if mode == "add" else "Update Data"
        mode_btn = page.locator(f'text="{mode_label}"')
        if await mode_btn.count() > 0:
            await mode_btn.first.click()
            await page.wait_for_timeout(1500)
            logger.info("Selected '%s'", mode_label)
    except Exception as e:
        logger.warning("Mode selection: %s", e)

    # Part B: Answer "WHAT ARE YOU GOING TO ADD?" dropdown
    # Opens a styled dropdown with options like:
    #   - "Uploading a new list not in DataSift yet"
    #   - "Adding properties to an existing list inside DataSift"
    #   - "Adding properties to owners (requires m. address)"
    try:
        # Click the dropdown to open it
        dropdown = page.locator('text="Select one option"')
        if await dropdown.count() > 0:
            await dropdown.first.click()
            await page.wait_for_timeout(1500)
            logger.debug("Opened upload type dropdown")

            await _screenshot(page, "step1_dropdown_opened")

            if existing_list:
                # Select "Adding properties to an existing list inside DataSift"
                existing_opt = page.locator('text="Adding properties to an existing list inside DataSift"')
                if await existing_opt.count() > 0:
                    await existing_opt.first.click()
                    await page.wait_for_timeout(1500)
                    logger.info("Selected 'Adding properties to an existing list inside DataSift'")
                else:
                    # Fallback: partial match
                    existing_opt = page.locator('text="Adding properties to an existing list"')
                    if await existing_opt.count() > 0:
                        await existing_opt.first.click()
                        await page.wait_for_timeout(1500)
                        logger.info("Selected 'Adding properties to an existing list' (partial match)")
            else:
                # Select "Uploading a new list not in DataSift yet"
                new_list = page.locator('text="Uploading a new list not in DataSift yet"')
                if await new_list.count() > 0:
                    await new_list.first.click()
                    await page.wait_for_timeout(1500)
                    logger.info("Selected 'Uploading a new list not in DataSift yet'")
                else:
                    # Fallback: try partial match
                    new_list = page.locator('text="Uploading a new list"')
                    if await new_list.count() > 0:
                        await new_list.first.click()
                        await page.wait_for_timeout(1500)
                        logger.info("Selected 'Uploading a new list' (partial match)")
    except Exception as e:
        logger.warning("Dropdown selection: %s", e)

    await _screenshot(page, "step1_setup_complete")

    # Part C: "LET'S STAY ORGANIZED" section + required list name
    # These fields appear after selecting upload type.

    # Dismiss notifications popup AGAIN if still there (it blocks interactions)
    try:
        no_thanks = page.locator('button:has-text("NO, THANKS")')
        if await no_thanks.count() > 0:
            await no_thanks.first.click()
            await page.wait_for_timeout(500)
    except Exception as e:
        logger.debug("Popup dismissal failed: %s", e)

    # "WHERE DID YOU PURCHASE THIS LIST?" — select "Other" or first option
    try:
        purchase_dropdown = page.locator('text="WHERE DID YOU PURCHASE THIS LIST?"').locator(
            '..').locator('text="Select an option"')
        if await purchase_dropdown.count() > 0:
            await purchase_dropdown.first.click()
            await page.wait_for_timeout(500)
            # Select "Other" if available, otherwise first option
            other = page.locator('text="Other"')
            if await other.count() > 0:
                await other.first.click()
            else:
                opts = page.locator('[class*="option"]')
                if await opts.count() > 0:
                    await opts.first.click()
            await page.wait_for_timeout(500)
    except Exception as e:
        logger.debug("Purchase dropdown: %s", e)

    # "DOES DATA CONTAIN PHONE NUMBERS?" — select "No"
    try:
        phone_dropdown = page.locator('text="DOES DATA CONTAIN PHONE NUMBERS?"').locator(
            '..').locator('text="Select an option"')
        if await phone_dropdown.count() > 0:
            await phone_dropdown.first.click()
            await page.wait_for_timeout(500)
            no_opt = page.locator('text="No"')
            if await no_opt.count() > 0:
                await no_opt.first.click()
            await page.wait_for_timeout(500)
    except Exception as e:
        logger.debug("Phone numbers dropdown: %s", e)

    # "ASSOCIATE DATA WITH LIST" — enter or search for list name
    try:
        if existing_list:
            # Existing list mode: styled dropdown showing "Select a list"
            # Click the dropdown to open it, then select the target list
            list_dropdown = page.locator('text="Select a list"')
            if await list_dropdown.count() > 0:
                await list_dropdown.first.click()
                await page.wait_for_timeout(2000)
                logger.debug("Opened existing list dropdown")
                await _screenshot(page, "step1_list_dropdown_opened")

                # Look for the target list name in the dropdown options
                match = page.locator(f'text="{list_name}"')
                if await match.count() > 0:
                    # Click the last match (dropdown option, not the label)
                    await match.last.click()
                    await page.wait_for_timeout(1000)
                    logger.info("Selected existing list: %s", list_name)
                else:
                    logger.warning("List '%s' not found in dropdown", list_name)
                    await _screenshot(page, "step1_list_not_found")
            else:
                # Fallback: try searching for existing list via input
                list_input = page.locator(
                    'input[placeholder*="Search"], '
                    'input[placeholder*="list"]'
                )
                if await list_input.count() > 0:
                    await list_input.first.fill(list_name or "")
                    await page.wait_for_timeout(2000)
                    match = page.locator(f'text="{list_name}"')
                    if await match.count() > 0:
                        await match.last.click()
                        await page.wait_for_timeout(1000)
                        logger.info("Selected existing list via search: %s", list_name)
        else:
            # New list mode: type a new list name
            list_input = page.locator('input[placeholder*="Enter new list name"], input[placeholder*="list name"]')
            if await list_input.count() > 0:
                if list_name is None:
                    from datetime import datetime as _dt
                    list_name = f"SiftStack {_dt.now().strftime('%Y-%m-%d')}"
                await list_input.first.fill(list_name)
                logger.info("Set list name: %s", list_name)
                await page.wait_for_timeout(500)
    except Exception as e:
        logger.debug("List name input: %s", e)

    await _screenshot(page, "step1_form_filled")

    # Click "Next Step" to proceed to step 2
    await _click_next_step(page, timeout=30000)

    # ── Wizard Step 2: Add tags ──
    logger.info("Wizard Step 2: Adding 'Courthouse Data' tag...")
    await page.wait_for_timeout(1000)
    await _screenshot(page, "step2_tags")

    # Add "Courthouse Data" tag via the Custom Tags input on the right side
    try:
        tag_input = page.locator('input[placeholder*="Search or add a new tag"]')
        if await tag_input.count() > 0:
            # Click input first, then type to trigger autocomplete dropdown
            await tag_input.first.click()
            await page.wait_for_timeout(500)
            await tag_input.first.fill("")
            await page.wait_for_timeout(300)
            await tag_input.first.type("Courthouse Data", delay=50)
            await page.wait_for_timeout(1500)
            await _screenshot(page, "step2_tag_typed")

            # Check if "Courthouse Data" appears in autocomplete dropdown — click it
            tag_option = page.locator('text="Courthouse Data"')
            tag_count = await tag_option.count()
            if tag_count > 1:
                # Multiple matches — click the one in the dropdown (not the input)
                await tag_option.nth(1).click()
                await page.wait_for_timeout(1000)
                logger.info("Selected 'Courthouse Data' from dropdown")
            elif tag_count == 1:
                # Check if it's the input value or a dropdown option
                tag_box = await tag_option.first.bounding_box()
                if tag_box and tag_box["y"] > 350:
                    # It's below the input — it's a dropdown option
                    await tag_option.first.click()
                    await page.wait_for_timeout(1000)
                    logger.info("Selected 'Courthouse Data' from dropdown")
                else:
                    # It's the input itself — use JS to click "Add" or press Enter
                    await tag_input.first.press("Enter")
                    await page.wait_for_timeout(1000)
                    logger.info("Added 'Courthouse Data' tag (via Enter)")
            else:
                # No dropdown match — click "Add" via JS to create new tag
                added = await page.evaluate('''() => {
                    const els = document.querySelectorAll('span, div, a, button, p');
                    for (const el of els) {
                        const text = el.textContent.trim();
                        const rect = el.getBoundingClientRect();
                        if (text === "Add" && rect.width > 0 && rect.width < 60
                            && rect.x > 700 && rect.y > 250 && rect.y < 400) {
                            el.click();
                            return true;
                        }
                    }
                    return false;
                }''')
                if added:
                    await page.wait_for_timeout(1000)
                    logger.info("Created 'Courthouse Data' tag via Add button")
                else:
                    await tag_input.first.press("Enter")
                    await page.wait_for_timeout(1000)
                    logger.info("Added 'Courthouse Data' tag (via Enter fallback)")

            await _screenshot(page, "step2_tag_added")
        else:
            logger.warning("Tag input not found — 'Courthouse Data' tag NOT added")
    except Exception as e:
        logger.warning("Tag addition failed: %s", e)

    await _click_next_step(page)

    # ── Wizard Step 3: Upload the file ──
    logger.info("Wizard Step 3: Uploading CSV file: %s", csv_path.name)
    await page.wait_for_timeout(3000)
    await _screenshot(page, "step3_before_upload")

    try:
        file_input = page.locator('input[type="file"]')
        # Retry with increasing waits for slow SPA rendering
        for wait in [3000, 5000, 8000]:
            if await file_input.count() > 0:
                break
            logger.debug("File input not found, waiting %dms...", wait)
            await page.wait_for_timeout(wait)
            file_input = page.locator('input[type="file"]')

        if await file_input.count() > 0:
            await file_input.first.set_input_files(str(csv_path))
            logger.info("CSV file selected: %s", csv_path.name)
            await page.wait_for_timeout(3000)
        else:
            await _screenshot(page, "step3_no_file_input")
            result["message"] = "Could not find file input element"
            logger.error(result["message"])
            return result
    except Exception as e:
        result["message"] = f"File upload failed: {e}"
        logger.error(result["message"])
        return result

    await _screenshot(page, "step3_file_uploaded")
    await _click_next_step(page)

    # ── Wizard Step 4: Map the columns ──
    logger.info("Wizard Step 4: Column mapping — mapping Tags and Lists...")
    await page.wait_for_timeout(3000)
    await _screenshot(page, "step4_column_mapping")

    # Try to drag unmapped columns (left side) to their targets (right side)
    # DataSift uses styled-components with draggable="false" — need slow mouse drag
    async def _drag_column(source_el, target_el):
        """Drag a CSV column card to a mapping target using slow mouse moves."""
        src_box = await source_el.bounding_box()
        dst_box = await target_el.bounding_box()
        if not src_box or not dst_box:
            return False
        sx = src_box["x"] + src_box["width"] / 2
        sy = src_box["y"] + src_box["height"] / 2
        dx = dst_box["x"] + dst_box["width"] / 2
        dy = dst_box["y"] + dst_box["height"] / 2
        await page.mouse.move(sx, sy)
        await page.wait_for_timeout(500)
        await page.mouse.down()
        await page.wait_for_timeout(500)
        steps = 20
        for i in range(1, steps + 1):
            frac = i / steps
            await page.mouse.move(
                sx + (dx - sx) * frac,
                sy + (dy - sy) * frac,
            )
            await page.wait_for_timeout(50)
        await page.wait_for_timeout(500)
        await page.mouse.up()
        await page.wait_for_timeout(1000)
        return True

    # Map Tags column: find "Tags" card on left, drag to "Tags" target on right
    for col_name in ["Tags", "Lists"]:
        try:
            # Source: unmapped column card on the left (contains column name + sample data)
            source = page.locator(f'div:has-text("{col_name}") >> visible=true').first
            # Target: mapping slot on the right side (search for it)
            # Right-side targets have the field name — search within right panel area
            target = page.locator(f'text="{col_name}"').last
            if await source.count() > 0 and await target.count() > 0:
                src_box = await source.bounding_box()
                tgt_box = await target.bounding_box()
                # Ensure source is on left (<600px) and target is on right (>600px)
                if src_box and tgt_box and src_box["x"] < 600 and tgt_box["x"] > 600:
                    if await _drag_column(source, target):
                        logger.info("Mapped column: %s", col_name)
                        await page.wait_for_timeout(1000)
                    else:
                        logger.warning("Drag failed for column: %s", col_name)
                else:
                    logger.debug("Column %s: no valid source/target positions", col_name)
            else:
                logger.debug("Column %s: source or target not found", col_name)
        except Exception as e:
            logger.warning("Column mapping %s failed: %s", col_name, e)

    await _screenshot(page, "step4_after_mapping")

    # Click Next Step to proceed past mapping
    await _click_next_step(page)
    await _screenshot(page, "step4_mapping_done")

    # ── Wizard Step 5: Review ──
    logger.info("Wizard Step 5: Review and finish upload...")
    await page.wait_for_timeout(2000)
    await _screenshot(page, "step5_review")

    try:
        finish_btn = page.locator(
            'button:has-text("Finish Upload"), '
            'button:has-text("Finish"), '
            'button:has-text("Submit")'
        )
        if await finish_btn.count() > 0:
            await finish_btn.first.click()
            logger.info("Clicked Finish Upload")
        else:
            await _screenshot(page, "step5_no_finish_btn")
            logger.warning("Finish Upload button not found")
    except Exception as e:
        logger.warning("Finish step: %s", e)

    # Wait for processing confirmation
    try:
        success_indicator = page.locator(
            'text="Upload Complete", '
            'text="successfully", '
            'text="records imported", '
            'text="records added", '
            'text="records uploaded"'
        )
        await success_indicator.first.wait_for(timeout=60000)
        success_text = await success_indicator.first.text_content()
        result["success"] = True
        result["message"] = success_text or "Upload completed"
        logger.info("DataSift upload complete: %s", result["message"])
    except PwTimeout:
        await _screenshot(page, "step5_timeout")
        result["message"] = "Upload may have succeeded but confirmation timed out — check Activity page"
        logger.warning(result["message"])
        result["success"] = True

    await _save_cookies(page)
    return result


DATASIFT_RECORDS_URL = "https://app.reisift.io/records/properties"


async def _dismiss_popups(page: Page) -> None:
    """Dismiss notification popups that may overlay buttons."""
    try:
        # "NO, THANKS" is NOT a <button> — it's a text/link element in the notification popup.
        # Try clicking ANY element with dismiss-like text (not just buttons).
        for text in ["NO, THANKS", "No, thanks", "No Thanks", "NO THANKS", "Not Now", "Dismiss"]:
            el = page.get_by_text(text, exact=True)
            if await el.count() > 0:
                await el.first.click(force=True)
                await page.wait_for_timeout(1000)
                logger.debug("Dismissed popup via '%s'", text)
                return

        # JavaScript fallback: remove the popup from the DOM entirely
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
            const els = document.querySelectorAll('[class*="notification"], [class*="Notification"], [class*="popup"], [class*="Popup"]');
            for (const el of els) {
                if (el.textContent && el.textContent.includes('notifications')) {
                    el.remove();
                    removed++;
                }
            }
            // Also try removing any fixed/absolute overlays at the top
            const overlays = document.querySelectorAll('[style*="position: fixed"], [style*="position:fixed"]');
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
        logger.debug("Popup JS removal failed: %s", e)


async def _navigate_to_records(page: Page) -> None:
    """Navigate to the Records page and wait for SPA to render."""
    if "/records" not in page.url:
        await page.goto(DATASIFT_RECORDS_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)
    await _dismiss_popups(page)


async def _filter_by_list(page: Page, list_name: str) -> bool:
    """Filter records page by list name. Returns True if filter applied.

    DataSift filter panel: right-side overlay opened by "Filter Records" link.
    Has an "Add new filter block" search input. Type "Lists" → select "All Lists (AND)"
    → pick the list name → close panel.
    """
    try:
        await _dismiss_popups(page)

        # Open filter panel — "Filter Records" is an <a> link at top-right
        filter_link = page.locator('#Records__Filters_Trigger')
        if await filter_link.count() == 0:
            filter_link = page.locator('a:has-text("Filter Records")')

        if await filter_link.count() > 0:
            await filter_link.first.click()
            await page.wait_for_timeout(2000)
            logger.debug("Opened filter panel")
        else:
            logger.warning("No Filter Records link found")
            return False

        await _dismiss_popups(page)
        await _screenshot(page, "filter_opened")

        # Type "Lists" in the "Add new filter block" search input
        filter_search = page.locator('#RecordsFilters__Filter_Blocks__Search')
        if await filter_search.count() == 0:
            filter_search = page.locator('input[placeholder*="filter block"]')

        if await filter_search.count() > 0:
            await filter_search.first.click()
            await filter_search.first.fill("Lists")
            await page.wait_for_timeout(1500)

            # Click "All Lists (AND)" in the suggestions dropdown (inside the overlay)
            all_lists = page.locator('text="All Lists (AND)"')
            if await all_lists.count() > 0:
                await all_lists.first.click()
                await page.wait_for_timeout(2000)
                logger.debug("Selected 'All Lists (AND)' filter block")
            else:
                # Try "Any Lists (OR)" as fallback
                any_lists = page.locator('text="Any Lists (OR)"')
                if await any_lists.count() > 0:
                    await any_lists.first.click()
                    await page.wait_for_timeout(2000)
                    logger.debug("Selected 'Any Lists (OR)' filter block")
        else:
            logger.warning("Filter block search input not found")

        await _screenshot(page, "filter_lists_block_added")

        # Now a list picker appears with "Search for lists..." input and a dropdown.
        # Type the list name to search, then click the matching option.
        list_search = page.locator('input[placeholder*="Search for lists"]')
        if await list_search.count() > 0:
            await list_search.first.fill(list_name)
            await page.wait_for_timeout(2000)

            await _screenshot(page, "filter_list_searched")

            # Click the matching list option in the dropdown
            list_option = page.locator(f'text="{list_name}"')
            if await list_option.count() > 0:
                # Use the last match (the one in the dropdown, not the input field)
                await list_option.last.click()
                await page.wait_for_timeout(1000)
                logger.info("Selected list filter: %s", list_name)
        else:
            logger.warning("'Search for lists...' input not found")

        await _screenshot(page, "filter_list_selected")

        # Click "Apply Filters" button at the bottom of the filter panel
        apply_btn = page.locator('text="Apply Filters"')
        if await apply_btn.count() > 0:
            await apply_btn.first.click()
            await page.wait_for_timeout(3000)
            logger.info("Applied filters")
        else:
            # Fallback: close panel with X button
            close_x = page.locator('[class*="Aside"] button:has-text("×")')
            if await close_x.count() > 0:
                await close_x.first.click()
            else:
                await page.keyboard.press("Escape")
            await page.wait_for_timeout(2000)

        await _screenshot(page, "filter_applied")
        return True
    except Exception as e:
        logger.warning("Filter by list failed: %s", e)
        await _screenshot(page, "filter_failed")
        return False


async def _select_all_records(page: Page) -> bool:
    """Select all records on the current page. Returns True if selected."""
    try:
        # Dismiss popups aggressively — the notification popup blocks all clicks
        await _dismiss_popups(page)
        await page.wait_for_timeout(1000)
        await _dismiss_popups(page)
        await page.wait_for_timeout(500)

        await _screenshot(page, "before_select_all")

        # Strategy 1: Find the header checkbox position via JS, then use Playwright
        # mouse.click to properly trigger React's event system.
        # The header checkbox is near the "OWNER" column header text.
        header_pos = await page.evaluate("""() => {
            // Find the OWNER header text element
            const allEls = document.querySelectorAll('*');
            for (const el of allEls) {
                if (el.textContent.trim() === 'OWNER' && el.children.length === 0) {
                    const rect = el.getBoundingClientRect();
                    // The header checkbox is in the same row, to the left
                    // Find the nearest checkbox (same vertical position)
                    const checkboxes = document.querySelectorAll('input[type="checkbox"]');
                    let best = null;
                    let bestDist = Infinity;
                    for (const cb of checkboxes) {
                        if (cb.classList.contains('react-toggle-screenreader-only')) continue;
                        const cbRect = cb.getBoundingClientRect();
                        // Must be roughly same Y position (within 30px) and to the left
                        const yDist = Math.abs(cbRect.top - rect.top);
                        if (yDist < 30 && cbRect.left < rect.left) {
                            if (yDist < bestDist) {
                                bestDist = yDist;
                                best = cbRect;
                            }
                        }
                    }
                    if (best) {
                        return {x: best.left + best.width/2, y: best.top + best.height/2};
                    }
                }
            }
            return null;
        }""")

        if header_pos:
            # Use Playwright mouse click which properly triggers React events
            await page.mouse.click(header_pos["x"], header_pos["y"])
            clicked_header = f"clicked at ({header_pos['x']:.0f}, {header_pos['y']:.0f})"
            logger.info("Clicked header checkbox via coordinates: %s", clicked_header)
            await page.wait_for_timeout(1500)
        else:
            clicked_header = None

        if clicked_header:
            logger.info("Clicked header checkbox via JS: %s", clicked_header)
            await page.wait_for_timeout(1500)
        else:
            # Strategy 2: Click each record checkbox individually via JS
            clicked_count = await page.evaluate("""() => {
                const checkboxes = document.querySelectorAll('input[type="checkbox"]');
                let clicked = 0;
                for (const cb of checkboxes) {
                    if (cb.classList.contains('react-toggle-screenreader-only')) continue;
                    cb.click();
                    clicked++;
                }
                return clicked;
            }""")
            logger.info("Clicked %d checkboxes via JS (all non-toggle)", clicked_count)
            await page.wait_for_timeout(1500)

        await _screenshot(page, "records_selected_header")

        # After checking the header checkbox, a "Select All X records" banner may appear
        select_all_link = page.locator('text="Select all"')
        if await select_all_link.count() > 0:
            await select_all_link.first.click()
            await page.wait_for_timeout(1000)
            logger.debug("Clicked 'Select all' records link")

        # Verify: check if Manage or Send To buttons are now visible
        manage_visible = await page.locator('button:has-text("Manage")').count() > 0
        send_to_visible = await page.locator('button:has-text("Send To")').count() > 0
        logger.info("After select: Manage visible=%s, Send To visible=%s", manage_visible, send_to_visible)

        await _screenshot(page, "records_selected")
        return manage_visible or send_to_visible
    except Exception as e:
        logger.warning("Select all records failed: %s", e)
        await _screenshot(page, "select_all_failed")
        return False


async def enrich_records(page: Page, list_name: str) -> dict:
    """Enrich uploaded records with DataSift's SiftMap property data.

    UI Flow: Records → Filter by list → Select all → Manage → Enrich Data
    → toggle "Enrich Property Information" ON → click "Enrich"

    Only enriches property info (beds, baths, Zestimate, sqft, sale history).
    Owner enrichment is OFF to protect our PR/DM contact mapping.

    Args:
        page: Logged-in Playwright page.
        list_name: Name of the list to filter and enrich.

    Returns:
        Dict with {success, message}.
    """
    result = {"success": False, "message": ""}
    logger.info("Starting DataSift enrichment for list: %s", list_name)

    try:
        # Navigate to Records
        await _navigate_to_records(page)

        # Filter to the uploaded list
        filtered = await _filter_by_list(page, list_name)
        if not filtered:
            result["message"] = "Could not filter to list for enrichment"
            logger.warning(result["message"])
            # Continue anyway — may enrich whatever is showing

        # Select all records
        selected = await _select_all_records(page)
        if not selected:
            result["message"] = "Could not select records for enrichment"
            logger.error(result["message"])
            return result

        # Click Manage dropdown
        manage_btn = page.locator('button:has-text("Manage")')
        if await manage_btn.count() == 0:
            manage_btn = page.locator('text="Manage"')
        if await manage_btn.count() > 0:
            await manage_btn.first.click()
            await page.wait_for_timeout(1500)
            logger.debug("Opened Manage dropdown")
        else:
            await _screenshot(page, "enrich_no_manage")
            result["message"] = "Could not find Manage button"
            logger.error(result["message"])
            return result

        await _screenshot(page, "enrich_manage_opened")

        # Click "Enrich records" option (exact text from Manage dropdown)
        enrich_option = page.locator('text="Enrich records"')
        if await enrich_option.count() == 0:
            enrich_option = page.locator('text="Enrich Records"')
        if await enrich_option.count() == 0:
            enrich_option = page.locator('text="Enrich Data"')
        if await enrich_option.count() > 0:
            await enrich_option.first.click()
            await page.wait_for_timeout(2000)
            logger.debug("Opened Enrich records modal")
        else:
            await _screenshot(page, "enrich_no_option")
            result["message"] = "Could not find 'Enrich records' option in Manage menu"
            logger.error(result["message"])
            return result

        await _screenshot(page, "enrich_modal")

        # Configure enrichment toggles via JavaScript.
        # The modal uses react-toggle components with hidden checkbox inputs.
        # We want: "Enrich Property Information" ON, "Enrich Owners" OFF, "Swap Owners" OFF.
        # Configure enrichment toggles via JavaScript.
        # Based on the Enrich Records modal structure, toggles appear next to their label text.
        # Use previousElementSibling text to identify each toggle.
        toggle_result = await page.evaluate("""() => {
            const results = {};
            const toggles = document.querySelectorAll('.react-toggle');
            for (const toggle of toggles) {
                // Check previous sibling for label text
                const prev = toggle.previousElementSibling;
                const prevText = prev ? prev.textContent.trim() : '';

                let name = null;
                if (prevText.includes('Enrich Property Information') || prevText.includes('Property Information')) {
                    name = 'Enrich Property Information';
                } else if (prevText.includes('Enrich Owners') || prevText === 'Enrich Owners') {
                    name = 'Enrich Owners';
                } else if (prevText.includes('Swap Owners') || prevText === 'Swap Owners') {
                    name = 'Swap Owners';
                }
                if (!name) {
                    // Also try next sibling or parent's other children
                    const next = toggle.nextElementSibling;
                    const nextText = next ? next.textContent.trim() : '';
                    if (nextText.includes('Property Information')) name = 'Enrich Property Information';
                    else if (nextText.includes('Enrich Owners')) name = 'Enrich Owners';
                    else if (nextText.includes('Swap Owners')) name = 'Swap Owners';
                }
                if (!name) continue;

                const isChecked = toggle.classList.contains('react-toggle--checked');
                const shouldBeOn = name === 'Enrich Property Information';

                if (shouldBeOn && !isChecked) {
                    toggle.click();
                    results[name] = 'turned ON';
                } else if (!shouldBeOn && isChecked) {
                    toggle.click();
                    results[name] = 'turned OFF';
                } else {
                    results[name] = shouldBeOn ? 'already ON' : 'already OFF';
                }
            }
            return results;
        }""")
        logger.info("Enrichment toggles: %s", toggle_result)
        await page.wait_for_timeout(1000)

        await _screenshot(page, "enrich_toggles_set")

        # Click "Enrich N Records" button to start processing
        enrich_btn = page.locator('button:has-text("Enrich")')
        if await enrich_btn.count() > 1:
            # Multiple matches — prefer the one with "Records" in text
            enrich_btn = page.locator('button:has-text("Records")')
        if await enrich_btn.count() == 0:
            enrich_btn = page.locator('button:has-text("Start Enrichment")')
        if await enrich_btn.count() > 0:
            await enrich_btn.first.click()
            logger.info("Clicked Enrich — processing started")
            await page.wait_for_timeout(3000)
        else:
            await _screenshot(page, "enrich_no_button")
            result["message"] = "Could not find Enrich button"
            logger.error(result["message"])
            return result

        await _screenshot(page, "enrich_submitted")

        # Enrichment runs in background — we don't need to wait for completion
        result["success"] = True
        result["message"] = "Enrichment started — track progress in Activity → Action Page"
        logger.info(result["message"])

    except Exception as e:
        result["message"] = f"Enrichment failed: {e}"
        logger.error(result["message"])
        await _screenshot(page, "enrich_error")

    return result


async def skip_trace_records(page: Page, list_name: str) -> dict:
    """Skip trace uploaded records for phone numbers + emails.

    UI Flow: Records → Filter by list → Select all → Send To → Skip Trace
    → agree to terms → add tag → click "Skip Trace Records"

    Uses the unlimited skip trace plan ($97/mo) — no per-record cost.

    Args:
        page: Logged-in Playwright page.
        list_name: Name of the list to filter and skip trace.

    Returns:
        Dict with {success, message}.
    """
    result = {"success": False, "message": ""}
    logger.info("Starting DataSift skip trace for list: %s", list_name)

    try:
        # Navigate to Records (may already be there from enrichment)
        await _navigate_to_records(page)

        # Filter to the uploaded list
        filtered = await _filter_by_list(page, list_name)
        if not filtered:
            logger.warning("Could not filter to list for skip trace — continuing anyway")

        # Select all records
        selected = await _select_all_records(page)
        if not selected:
            result["message"] = "Could not select records for skip trace"
            logger.error(result["message"])
            return result

        # Click "Send To" dropdown
        send_to_btn = page.locator('button:has-text("Send To")')
        if await send_to_btn.count() == 0:
            send_to_btn = page.locator('button:has-text("Send to")')
        if await send_to_btn.count() == 0:
            send_to_btn = page.locator('text="Send To"')
        if await send_to_btn.count() > 0:
            await send_to_btn.first.click()
            await page.wait_for_timeout(1500)
            logger.debug("Opened Send To dropdown")
        else:
            await _screenshot(page, "skip_no_send_to")
            result["message"] = "Could not find 'Send To' button"
            logger.error(result["message"])
            return result

        await _screenshot(page, "skip_send_to_opened")

        # Click "Skip Trace" option
        skip_option = page.locator('text="Skip Trace"')
        if await skip_option.count() == 0:
            skip_option = page.locator('text="Skip trace"')
        if await skip_option.count() > 0:
            await skip_option.first.click()
            await page.wait_for_timeout(2000)
            logger.debug("Opened Skip Trace modal")
        else:
            await _screenshot(page, "skip_no_option")
            result["message"] = "Could not find 'Skip Trace' option in Send To menu"
            logger.error(result["message"])
            return result

        await _screenshot(page, "skip_modal")

        # Skip Trace modal is a 3-step wizard: Terms → Review → Sent!
        # Step 1: Click "I Agree with the terms" button
        agree_btn = page.locator('button:has-text("I Agree with the terms")')
        if await agree_btn.count() == 0:
            agree_btn = page.locator('button:has-text("I Agree")')
        if await agree_btn.count() > 0:
            await agree_btn.first.click()
            logger.info("Clicked 'I Agree with the terms'")
            await page.wait_for_timeout(2000)
        else:
            await _screenshot(page, "skip_no_agree")
            result["message"] = "Could not find 'I Agree with the terms' button"
            logger.error(result["message"])
            return result

        await _screenshot(page, "skip_review_step")

        # Step 2: Review step — may have tag input and a "Skip Trace" / confirm button
        # Add a custom tag (optional — DataSift auto-tags too)
        tag_input = page.locator('input[placeholder*="tag"], input[placeholder*="Tag"], input[placeholder*="Add tag"]')
        if await tag_input.count() > 0:
            from datetime import datetime as _dt
            tag = f"skip_traced_{_dt.now().strftime('%Y-%m')}"
            await tag_input.first.fill(tag)
            await page.wait_for_timeout(500)
            await tag_input.first.press("Enter")
            await page.wait_for_timeout(500)
            logger.info("Added skip trace tag: %s", tag)

        await _screenshot(page, "skip_ready")

        # Click the confirm/submit button to start skip trace
        # Try multiple possible button texts
        for btn_text in ["Skip Trace", "Skip Trace Records", "Start Skip Trace", "Submit", "Confirm", "Process"]:
            skip_btn = page.locator(f'button:has-text("{btn_text}")')
            if await skip_btn.count() > 0:
                await skip_btn.first.click()
                logger.info("Clicked '%s' — processing started", btn_text)
                await page.wait_for_timeout(3000)
                break
        else:
            await _screenshot(page, "skip_no_button")
            result["message"] = "Could not find skip trace submit button"
            logger.error(result["message"])
            return result

        await _screenshot(page, "skip_submitted")

        # Skip trace runs in background — we don't need to wait
        result["success"] = True
        result["message"] = "Skip trace started — track progress in Activity → Skip Trace tab"
        logger.info(result["message"])

    except Exception as e:
        result["message"] = f"Skip trace failed: {e}"
        logger.error(result["message"])
        await _screenshot(page, "skip_error")

    return result


async def upload_to_datasift(
    csv_path: Path,
    email: str | None = None,
    password: str | None = None,
    headless: bool = True,
    enrich: bool = True,
    skip_trace: bool = True,
) -> dict:
    """Full DataSift workflow: launch browser → login → upload CSV → enrich → skip trace.

    Args:
        csv_path: Path to DataSift-formatted CSV.
        email: DataSift login email (defaults to DATASIFT_EMAIL env var).
        password: DataSift login password (defaults to DATASIFT_PASSWORD env var).
        headless: Run browser in headless mode.
        enrich: Run "Enrich Property Information" after upload (default True).
        skip_trace: Run "Skip Trace" after upload (default True, uses unlimited plan).

    Returns:
        Dict with upload results including enrich_result and skip_trace_result.
    """
    email = email or os.environ.get("DATASIFT_EMAIL", "")
    password = password or os.environ.get("DATASIFT_PASSWORD", "")

    if not email or not password:
        return {
            "success": False,
            "records_uploaded": 0,
            "errors": 0,
            "message": "DATASIFT_EMAIL and DATASIFT_PASSWORD must be set",
        }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            # Login
            logged_in = await login(page, email, password)
            if not logged_in:
                return {
                    "success": False,
                    "records_uploaded": 0,
                    "errors": 0,
                    "message": "DataSift login failed",
                }

            # Upload CSV
            result = await upload_csv(page, csv_path)

            if result.get("success"):
                # Derive list name (same format as upload_csv generates)
                from datetime import datetime as _dt
                list_name = f"SiftStack {_dt.now().strftime('%Y-%m-%d')}"

                # Enrich property data via SiftMap
                if enrich:
                    enrich_result = await enrich_records(page, list_name)
                    result["enrich_result"] = enrich_result
                    logger.info("Enrichment: %s", enrich_result.get("message", ""))

                # Skip trace for phones + emails
                if skip_trace:
                    skip_result = await skip_trace_records(page, list_name)
                    result["skip_trace_result"] = skip_result
                    logger.info("Skip trace: %s", skip_result.get("message", ""))

            return result

        finally:
            await browser.close()


async def upload_datasift_split(
    csv_infos: list[dict],
    email: str | None = None,
    password: str | None = None,
    headless: bool = True,
    enrich: bool = True,
    skip_trace: bool = True,
    existing_list: bool = False,
) -> dict:
    """Upload multiple CSVs sequentially for split Message Board entries.

    Uploads each CSV in a single browser session, then runs enrich + skip trace
    once after all uploads complete.

    Args:
        csv_infos: List of dicts from write_datasift_split_csvs(), each with
            "path", "label", and "list_name" keys.
        email: DataSift login email (defaults to DATASIFT_EMAIL env var).
        password: DataSift login password (defaults to DATASIFT_PASSWORD env var).
        headless: Run browser in headless mode.
        enrich: Run enrichment after all uploads (default True).
        skip_trace: Run skip trace after all uploads (default True).
        existing_list: If True, target existing lists instead of creating new ones.

    Returns:
        Dict with per-upload results, enrich_result, and skip_trace_result.
    """
    email = email or os.environ.get("DATASIFT_EMAIL", "")
    password = password or os.environ.get("DATASIFT_PASSWORD", "")

    if not email or not password:
        return {
            "success": False,
            "message": "DATASIFT_EMAIL and DATASIFT_PASSWORD must be set",
            "uploads": [],
        }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            # Login once
            logged_in = await login(page, email, password)
            if not logged_in:
                return {
                    "success": False,
                    "message": "DataSift login failed",
                    "uploads": [],
                }

            # Upload each CSV sequentially
            uploads = []
            all_success = True
            for i, info in enumerate(csv_infos):
                label = info["label"]
                logger.info("Uploading CSV %d/%d (%s): %s",
                            i + 1, len(csv_infos), label, info["path"])

                result = await upload_csv(
                    page, info["path"], list_name=info["list_name"],
                    existing_list=existing_list,
                )
                result["label"] = label
                uploads.append(result)

                if not result.get("success"):
                    logger.error("Upload %s failed: %s", label, result.get("message"))
                    all_success = False
                    break

                # Wait between uploads so DataSift can process/index
                if i < len(csv_infos) - 1:
                    logger.info("Waiting 15s before next upload...")
                    await page.wait_for_timeout(15000)

            combined = {
                "success": all_success,
                "uploads": uploads,
                "message": f"Uploaded {len(uploads)}/{len(csv_infos)} CSVs",
            }

            # Enrich + skip trace once, using the first list name (has all records)
            if all_success and csv_infos:
                first_list = csv_infos[0]["list_name"]

                if enrich:
                    enrich_result = await enrich_records(page, first_list)
                    combined["enrich_result"] = enrich_result
                    logger.info("Enrichment: %s", enrich_result.get("message", ""))

                if skip_trace:
                    skip_result = await skip_trace_records(page, first_list)
                    combined["skip_trace_result"] = skip_result
                    logger.info("Skip trace: %s", skip_result.get("message", ""))

            return combined

        finally:
            await browser.close()


# ── Phone Tag Export & Upload ────────────────────────────────────────────


async def export_phone_enrichment(
    page: Page,
    *,
    list_name: str | None = None,
    preset_folder: str | None = None,
    all_records: bool = False,
    download_dir: str | None = None,
) -> dict:
    """Export phone enrichment CSV from DataSift via Playwright.

    Navigates to Records, applies filters (list/preset/none), selects all records,
    clicks Manage → Export, and downloads the Phone Enrichment CSV.

    Args:
        page: Logged-in Playwright page.
        list_name: Filter to a specific list name.
        preset_folder: Filter using a saved filter preset folder name.
        all_records: If True, export all records (no filter).
        download_dir: Directory for downloads (defaults to output/).

    Returns:
        Dict with {success, message, download_path}.
    """
    result = {"success": False, "message": "", "download_path": None}

    if download_dir is None:
        from config import OUTPUT_DIR
        download_dir = str(OUTPUT_DIR)

    try:
        # Navigate to Records
        await _navigate_to_records(page)

        # Apply filter based on targeting mode
        if list_name:
            filtered = await _filter_by_list(page, list_name)
            if not filtered:
                result["message"] = f"Could not filter to list: {list_name}"
                logger.warning(result["message"])
        elif preset_folder:
            filtered = await _filter_by_preset(page, preset_folder)
            if not filtered:
                result["message"] = f"Could not filter to preset: {preset_folder}"
                logger.warning(result["message"])
        elif all_records:
            logger.info("Exporting all records (no filter)")
        else:
            result["message"] = "No targeting mode specified"
            logger.error(result["message"])
            return result

        await page.wait_for_timeout(2000)

        # Select all records
        selected = await _select_all_records(page)
        if not selected:
            result["message"] = "Could not select records for export"
            logger.error(result["message"])
            return result

        # Click Manage → Export
        manage_btn = page.locator('button:has-text("Manage")')
        if await manage_btn.count() == 0:
            manage_btn = page.locator('text="Manage"')
        if await manage_btn.count() > 0:
            await manage_btn.first.click()
            await page.wait_for_timeout(1500)
            logger.debug("Opened Manage dropdown")
        else:
            await _screenshot(page, "export_no_manage")
            result["message"] = "Could not find Manage button"
            logger.error(result["message"])
            return result

        await _screenshot(page, "export_manage_opened")

        # Click "Export" option from Manage dropdown
        export_option = page.locator('text="Export"')
        if await export_option.count() > 0:
            await export_option.first.click()
            await page.wait_for_timeout(2000)
            logger.debug("Clicked Export option")
        else:
            await _screenshot(page, "export_no_option")
            result["message"] = "Could not find 'Export' option in Manage menu"
            logger.error(result["message"])
            return result

        await _screenshot(page, "export_modal")

        # Export Selection wizard:
        #   Step 1: Phone type/status filters → click "Next"
        #   Step 2: Enter filename (optional) → click "Export N records"
        # The export may trigger a direct download or process in background.

        await _screenshot(page, "export_wizard_step1")

        # Step 1: Click "Next" to advance past phone filters
        next_btn = page.locator('button:has-text("Next")')
        if await next_btn.count() > 0:
            logger.info("Export wizard step 1 — clicking Next")
            await next_btn.first.click()
            await page.wait_for_timeout(2000)
        else:
            await _screenshot(page, "export_wizard_no_next")
            result["message"] = "Export wizard: no Next button on step 1"
            logger.error(result["message"])
            return result

        await _screenshot(page, "export_wizard_step2")

        # Step 2: Optional filename input, then "Export N records" button
        # Fill in a filename so we can find the download
        filename_input = page.locator('input[placeholder*="filename"]')
        if await filename_input.count() > 0:
            await filename_input.first.fill("phone_enrichment_export")
            logger.info("Set export filename to 'phone_enrichment_export'")
            await page.wait_for_timeout(500)

        # Click the "Export N records" button — try to catch a download
        export_btn = page.locator('button:has-text("Export")')
        if await export_btn.count() == 0:
            await _screenshot(page, "export_wizard_no_export_btn")
            result["message"] = "Export wizard: no Export button on step 2"
            logger.error(result["message"])
            return result

        # Try to catch a direct download (some exports trigger immediately)
        try:
            async with page.expect_download(timeout=30000) as download_info:
                await export_btn.first.click()
                logger.info("Clicked Export button — waiting for download...")

            download = await download_info.value
            save_path = os.path.join(
                download_dir, download.suggested_filename or "phone_enrichment_export.csv"
            )
            await download.save_as(save_path)
            result["success"] = True
            result["download_path"] = save_path
            result["message"] = f"Exported to {save_path}"
            logger.info("Phone enrichment export saved: %s", save_path)
            return result

        except Exception:
            logger.info("No immediate download — export may be processing in background")

        # Export is background-processed: check Activity tab or wait for download link
        await _screenshot(page, "export_after_click")
        await page.wait_for_timeout(3000)

        # Check if a success notification appeared with a download link
        download_link = page.locator('a:has-text("download")')
        if await download_link.count() == 0:
            download_link = page.locator('a:has-text("Download")')
        if await download_link.count() == 0:
            download_link = page.locator('a[href*="export"]')
        if await download_link.count() == 0:
            download_link = page.locator('a[href*="csv"]')

        if await download_link.count() > 0:
            logger.info("Found download link — clicking")
            try:
                async with page.expect_download(timeout=60000) as download_info:
                    await download_link.first.click()
                download = await download_info.value
                save_path = os.path.join(
                    download_dir, download.suggested_filename or "phone_enrichment_export.csv"
                )
                await download.save_as(save_path)
                result["success"] = True
                result["download_path"] = save_path
                result["message"] = f"Exported to {save_path}"
                logger.info("Phone enrichment export saved: %s", save_path)
                return result
            except Exception as dl_err:
                logger.warning("Download link click failed: %s", dl_err)

        # Export is queued in background — find it in Activity → Download tab
        logger.info("Checking Activity → Download tab for export...")

        # Navigate to Activity page
        await page.goto("https://app.reisift.io/activity", wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        # Click the "Download" tab (not the default "Actions" tab)
        download_tab = page.locator('text="Download"')
        if await download_tab.count() > 0:
            await download_tab.first.click()
            await page.wait_for_timeout(3000)
            logger.info("Switched to Download tab")
        else:
            logger.warning("Could not find Download tab")

        await _screenshot(page, "export_download_tab")

        # Wait for export to finish processing, then click the first "Download" action
        # The Download tab has rows with: FILENAME | TOTAL | PROCESSED | DATE | STATUS | ACTIONS
        # Each completed row has a "Download" link in ACTIONS column
        # Note: "Complete" is inside a styled badge/button — use broader matching
        for poll in range(12):
            await page.wait_for_timeout(5000)
            await _screenshot(page, f"export_download_poll{poll + 1}")

            # Check if our export row is ready — look for "Download" action links
            # or "Complete" in any form (button, badge, span)
            complete_count = await page.locator('text=/Complete/').count()
            download_action_count = await page.locator('text="Download"').count()
            logger.info("Poll %d: %d Complete indicators, %d Download links",
                        poll + 1, complete_count, download_action_count)

            # If we see Download action links (beyond the tab header), proceed
            # The "Download" tab header is 1, so action links start at 2+
            if download_action_count > 1:
                logger.info("Export rows found — attempting download from first row")

                # The "Download" text appears in both the tab header AND row action links.
                # Skip the first match (tab header) and click the second (first row action).
                dl_links = page.locator('text="Download"')
                # Index 0 = tab header, index 1 = first row's Download action
                first_row_dl = dl_links.nth(1)

                # Debug: log what element we found
                tag = await first_row_dl.evaluate("el => el.tagName + ' | ' + el.outerHTML.substring(0, 200)")
                logger.info("First row Download element: %s", tag)

                # Strategy 1: expect_download + click
                try:
                    async with page.expect_download(timeout=15000) as download_info:
                        await first_row_dl.click()
                    download = await download_info.value
                    save_path = os.path.join(
                        download_dir,
                        download.suggested_filename or "phone_enrichment_export.csv",
                    )
                    await download.save_as(save_path)
                    result["success"] = True
                    result["download_path"] = save_path
                    result["message"] = f"Exported to {save_path}"
                    logger.info("Phone enrichment export saved: %s", save_path)
                    return result
                except Exception as dl_err:
                    logger.info("expect_download failed: %s — trying popup handler", str(dl_err)[:80])

                # Strategy 2: The click may have opened a new tab/popup with the file
                await page.wait_for_timeout(2000)
                pages = page.context.pages
                if len(pages) > 1:
                    new_page = pages[-1]
                    logger.info("New tab opened: %s", new_page.url[:100])
                    # If the new tab URL is a file/API endpoint, fetch its content
                    try:
                        content = await new_page.content()
                        save_path = os.path.join(download_dir, "phone_enrichment_export.csv")
                        # Check if it looks like CSV (not HTML)
                        if content.strip().startswith(("Phone", '"Phone', "phone", '"phone')):
                            with open(save_path, "w", encoding="utf-8") as f:
                                f.write(content)
                            result["success"] = True
                            result["download_path"] = save_path
                            result["message"] = f"Exported to {save_path}"
                            logger.info("Saved CSV from new tab: %s", save_path)
                            await new_page.close()
                            return result
                        # If it's not CSV text, try getting the URL and fetching via API
                        tab_url = new_page.url
                        await new_page.close()
                        if tab_url and "reisift" in tab_url:
                            logger.info("Fetching file from tab URL: %s", tab_url[:100])
                            import requests as req_lib
                            cookies = await page.context.cookies()
                            cookies_dict = {c["name"]: c["value"] for c in cookies}
                            resp = req_lib.get(tab_url, cookies=cookies_dict, timeout=60)
                            if resp.status_code == 200:
                                with open(save_path, "wb") as f:
                                    f.write(resp.content)
                                result["success"] = True
                                result["download_path"] = save_path
                                result["message"] = f"Exported to {save_path}"
                                logger.info("Saved CSV via HTTP from tab URL: %s", save_path)
                                return result
                    except Exception as tab_err:
                        logger.warning("New tab handling failed: %s", tab_err)

                # Strategy 3: Use Playwright API request context with browser cookies
                try:
                    # Get the download URL from the element's data attributes or onclick
                    dl_info = await first_row_dl.evaluate("""el => {
                        return {
                            href: el.href || el.getAttribute('href') || '',
                            onclick: el.getAttribute('onclick') || '',
                            dataUrl: el.getAttribute('data-url') || el.getAttribute('data-href') || '',
                            parentHtml: el.parentElement ? el.parentElement.outerHTML.substring(0, 500) : ''
                        }
                    }""")
                    logger.info("Download element info: %s", dl_info)

                    url = dl_info.get("href") or dl_info.get("dataUrl")
                    if url and url != "#" and url != "":
                        import requests as req_lib
                        cookies = await page.context.cookies()
                        cookies_dict = {c["name"]: c["value"] for c in cookies}
                        save_path = os.path.join(download_dir, "phone_enrichment_export.csv")
                        resp = req_lib.get(url, cookies=cookies_dict, timeout=60)
                        if resp.status_code == 200:
                            with open(save_path, "wb") as f:
                                f.write(resp.content)
                            result["success"] = True
                            result["download_path"] = save_path
                            result["message"] = f"Exported to {save_path}"
                            logger.info("Saved CSV via direct URL: %s", save_path)
                            return result
                except Exception as info_err:
                    logger.warning("Element info extraction failed: %s", info_err)

                await _screenshot(page, "export_download_failed")
                break  # Download action links found but all strategies failed

            # Still processing
            processing = page.locator('text=/[Pp]rocessing|[Pp]ending|[Qq]ueued/')
            if await processing.count() > 0:
                logger.info("Export still processing (poll %d/12)...", poll + 1)
            else:
                logger.info("Waiting for export to appear (poll %d/12)...", poll + 1)

        await _screenshot(page, "export_download_final")

        result["message"] = (
            "Export triggered but download not captured. "
            "Check DataSift Activity tab manually and use --csv-path with the downloaded file."
        )
        logger.warning(result["message"])

    except Exception as e:
        result["message"] = f"Export failed: {e}"
        logger.error(result["message"])
        await _screenshot(page, "export_error")

    return result


async def _filter_by_preset(page: Page, preset_name: str) -> bool:
    """Filter records by a saved filter preset folder name."""
    try:
        await _dismiss_popups(page)

        # Open filter panel
        filter_link = page.locator('#Records__Filters_Trigger')
        if await filter_link.count() == 0:
            filter_link = page.locator('a:has-text("Filter Records")')

        if await filter_link.count() > 0:
            await filter_link.first.click()
            await page.wait_for_timeout(2000)
            logger.debug("Opened filter panel for preset")
        else:
            logger.warning("No Filter Records link found")
            return False

        await _dismiss_popups(page)
        await _screenshot(page, "preset_filter_opened")

        # Look for saved filter presets — click the preset name
        preset_option = page.locator(f'text="{preset_name}"')
        if await preset_option.count() > 0:
            await preset_option.first.click()
            await page.wait_for_timeout(2000)
            logger.info("Selected filter preset: %s", preset_name)

            apply_btn = page.locator('text="Apply Filters"')
            if await apply_btn.count() > 0:
                await apply_btn.first.click()
                await page.wait_for_timeout(3000)
                logger.info("Applied preset filters")
            else:
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(2000)

            await _screenshot(page, "preset_applied")
            return True
        else:
            logger.warning("Preset '%s' not found in filter panel", preset_name)
            await _screenshot(page, "preset_not_found")
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(1000)
            return False

    except Exception as e:
        logger.warning("Filter by preset failed: %s", e)
        await _screenshot(page, "preset_failed")
        return False


async def upload_phone_tags(page: Page, csv_path: str | Path) -> dict:
    """Upload phone tags CSV to DataSift via "Update Data → Tag phones by phone number".

    Args:
        page: Logged-in Playwright page.
        csv_path: Path to phone_tags_for_datasift.csv (Phone Number | Phone Tag).

    Returns:
        Dict with {success, message}.
    """
    result = {"success": False, "message": ""}
    csv_path = Path(csv_path)

    if not csv_path.exists():
        result["message"] = f"Phone tags CSV not found: {csv_path}"
        logger.error(result["message"])
        return result

    try:
        # Navigate to Records/Upload area
        if "/records" not in page.url:
            await page.goto(DATASIFT_RECORDS_URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)

        await _dismiss_popups(page)

        # Click "Upload File" in sidebar
        upload_link = page.locator('text="Upload File"')
        if await upload_link.count() == 0:
            upload_link = page.locator('a:has-text("Upload")')
        if await upload_link.count() > 0:
            await upload_link.first.click()
            await page.wait_for_timeout(2000)
            logger.debug("Clicked 'Upload File'")
        else:
            await _screenshot(page, "phone_tags_no_upload_link")
            result["message"] = "Could not find 'Upload File' link"
            logger.error(result["message"])
            return result

        await _screenshot(page, "phone_tags_upload_page")

        # Click "Update Data" (not "Add Data")
        update_btn = page.locator('text="Update Data"')
        if await update_btn.count() > 0:
            await update_btn.first.click()
            await page.wait_for_timeout(2000)
            logger.info("Selected 'Update Data' mode")
        else:
            await _screenshot(page, "phone_tags_no_update_data")
            result["message"] = "Could not find 'Update Data' button"
            logger.error(result["message"])
            return result

        await _screenshot(page, "phone_tags_update_data")

        # After clicking "Update Data", a dropdown appears:
        #   "WHAT ARE YOU GOING TO UPDATE?" → "Select one or more options"
        # Click the dropdown to open it, then select the phone tagging option.
        dropdown = page.locator('text="Select one or more options"')
        if await dropdown.count() == 0:
            # Try other dropdown selectors
            dropdown = page.locator('[class*="select"], [class*="Select"], [class*="dropdown"]')
        if await dropdown.count() > 0:
            await dropdown.first.click()
            await page.wait_for_timeout(1500)
            logger.info("Opened update options dropdown")
        else:
            logger.warning("No dropdown found, looking for options directly")

        await _screenshot(page, "phone_tags_dropdown_opened")

        # Log all visible options for debugging
        options_text = await page.evaluate("""() => {
            const items = document.querySelectorAll(
                '[class*="option"], [class*="Option"], [class*="menu"] span, ' +
                '[class*="Menu"] span, [role="option"], [role="listbox"] > *, li'
            );
            return Array.from(items)
                .map(el => el.textContent.trim())
                .filter(t => t.length > 0 && t.length < 100);
        }""")
        logger.info("Dropdown options found: %s", options_text)

        # Look for the phone tagging option — try exact and partial matches
        # Known options include "Tagging phone numbers by property address"
        # and possibly "Tagging phone numbers by phone number"
        tag_option = None
        for text_match in [
            "Tag phones by phone number",
            "Tagging phone numbers by phone number",
            "Tag phone numbers by phone number",
        ]:
            loc = page.locator(f'text="{text_match}"')
            if await loc.count() > 0:
                tag_option = loc.first
                logger.info("Found exact match: '%s'", text_match)
                break

        if tag_option is None:
            # Fallback: look for any option mentioning phone + number (not address)
            loc = page.locator('text=/[Tt]ag.*phone.*number/')
            if await loc.count() > 0:
                # Filter out "by property address" variant
                for i in range(await loc.count()):
                    opt = loc.nth(i)
                    opt_text = await opt.text_content()
                    if "address" not in opt_text.lower():
                        tag_option = opt
                        logger.info("Found regex match: '%s'", opt_text)
                        break
                if tag_option is None:
                    # If only "by property address" exists, that may be the only option
                    # DataSift may have renamed/combined the feature
                    tag_option = loc.first
                    opt_text = await tag_option.text_content()
                    logger.info("Using available option: '%s'", opt_text)

        if tag_option is None:
            # Last resort: look for any option with "phone" and "tag"
            loc = page.locator('text=/[Pp]hone.*[Tt]ag|[Tt]ag.*[Pp]hone/')
            if await loc.count() > 0:
                tag_option = loc.first
                opt_text = await tag_option.text_content()
                logger.info("Last resort match: '%s'", opt_text)

        if tag_option is not None:
            await tag_option.click(force=True)
            await page.wait_for_timeout(2000)
            logger.info("Selected phone tagging option")
        else:
            await _screenshot(page, "phone_tags_no_tag_option")
            result["message"] = "Could not find phone tagging option in dropdown"
            logger.error(result["message"])
            return result

        await _screenshot(page, "phone_tags_option_selected")

        # Click "Next Step" to advance past Setup
        await _click_next_step(page, timeout=10000)
        await page.wait_for_timeout(2000)
        await _screenshot(page, "phone_tags_after_setup")

        # Upload the CSV file
        file_input = page.locator('input[type="file"]')
        if await file_input.count() > 0:
            await file_input.first.set_input_files(str(csv_path.resolve()))
            await page.wait_for_timeout(3000)
            logger.info("Uploaded phone tags file: %s", csv_path.name)
        else:
            await _screenshot(page, "phone_tags_no_file_input")
            result["message"] = "Could not find file input for upload"
            logger.error(result["message"])
            return result

        await _screenshot(page, "phone_tags_file_uploaded")

        # Navigate through the remaining wizard steps using "Next Step" button.
        # Wizard flow: Setup ✓ → Upload the file ✓ → Map the columns → Review → Finish
        # The columns auto-map (Phone Number → Phone Number, Phone Tag → Phone Tags)
        # so we just click Next Step through mapping and review.
        max_steps = 5
        for step_num in range(max_steps):
            await _screenshot(page, f"phone_tags_wizard_step{step_num + 1}")

            # Check if we've reached a "Finish Upload" button (final step)
            finish_btn = page.locator('button:has-text("Finish Upload")')
            if await finish_btn.count() > 0:
                await finish_btn.first.click()
                await page.wait_for_timeout(5000)
                logger.info("Clicked 'Finish Upload' for phone tags")
                await _screenshot(page, "phone_tags_completed")
                break

            # Otherwise, click "Next Step" to advance
            next_btn = page.locator('button:has-text("Next Step")')
            if await next_btn.count() == 0:
                next_btn = page.locator('button:has-text("Next")')
            if await next_btn.count() > 0:
                await next_btn.first.click()
                await page.wait_for_timeout(3000)
                logger.info("Upload wizard step %d — clicked Next Step", step_num + 1)
            else:
                await _screenshot(page, f"phone_tags_wizard_stuck{step_num + 1}")
                logger.warning("No Next Step or Finish button at step %d", step_num + 1)
                break

        await _screenshot(page, "phone_tags_final")

        result["success"] = True
        result["message"] = f"Phone tags uploaded: {csv_path.name}"
        logger.info(result["message"])

    except Exception as e:
        result["message"] = f"Phone tag upload failed: {e}"
        logger.error(result["message"])
        await _screenshot(page, "phone_tags_error")

    return result


async def run_phone_validation_workflow(
    *,
    list_name: str | None = None,
    preset_folder: str | None = None,
    all_records: bool = False,
    csv_path: str | None = None,
    upload_tags: bool = True,
    email: str | None = None,
    password: str | None = None,
    headless: bool = False,
    api_key: str | None = None,
    tiers: dict | None = None,
    add_litigator: bool = False,
    batch_size: int = 10,
) -> dict:
    """Full phone validation workflow: export → validate → upload tags.

    Top-level orchestrator for the phone-validate CLI command.

    Args:
        list_name: DataSift list to export phones from.
        preset_folder: DataSift preset folder to use.
        all_records: Export all records (no filter).
        csv_path: Use a local CSV instead of exporting from DataSift.
        upload_tags: Upload phone tags back to DataSift after validation.
        email: DataSift login email.
        password: DataSift login password.
        headless: Run browser headless (default False for this workflow).
        api_key: Trestle API key.
        tiers: Custom tier definitions.
        add_litigator: Include litigator risk check.
        batch_size: Concurrent API requests.

    Returns:
        Dict with workflow results.
    """
    from phone_validator import run_phone_validation

    import config as _cfg

    email = email or _cfg.DATASIFT_EMAIL
    password = password or _cfg.DATASIFT_PASSWORD
    api_key = api_key or _cfg.TRESTLE_API_KEY

    result = {"success": False, "message": ""}

    # If CSV path provided, skip the Playwright export step
    if csv_path:
        phone_csv_path = csv_path
        logger.info("Using local CSV: %s", csv_path)
    else:
        # Need Playwright to export from DataSift
        if not email or not password:
            result["message"] = "DATASIFT_EMAIL and DATASIFT_PASSWORD required for export"
            logger.error(result["message"])
            return result

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            page = await context.new_page()

            try:
                logged_in = await login(page, email, password)
                if not logged_in:
                    result["message"] = "DataSift login failed"
                    return result

                export_result = await export_phone_enrichment(
                    page,
                    list_name=list_name,
                    preset_folder=preset_folder,
                    all_records=all_records,
                )
                if not export_result.get("success"):
                    result["message"] = f"Export failed: {export_result.get('message')}"
                    return result

                phone_csv_path = export_result["download_path"]
                result["export_result"] = export_result
            finally:
                await browser.close()

    # Run phone validation (Trestle API)
    if not api_key:
        result["message"] = "No Trestle API key. Set TRESTLE_API_KEY in .env."
        logger.error(result["message"])
        return result

    validation_result = run_phone_validation(
        csv_path=phone_csv_path,
        api_key=api_key,
        tiers=tiers,
        add_litigator=add_litigator,
        batch_size=batch_size,
    )
    result["validation_result"] = validation_result

    if not validation_result.get("success"):
        result["message"] = f"Validation failed: {validation_result.get('message')}"
        return result

    tag_csv_path = validation_result["tag_csv_path"]

    # Upload phone tags back to DataSift
    if upload_tags and tag_csv_path:
        if not email or not password:
            logger.warning("Skipping tag upload — no DataSift credentials")
        else:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=headless)
                context = await browser.new_context(
                    viewport={"width": 1280, "height": 720},
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                )
                page = await context.new_page()

                try:
                    logged_in = await login(page, email, password)
                    if not logged_in:
                        result["message"] = "DataSift login failed for tag upload"
                        return result

                    upload_result = await upload_phone_tags(page, tag_csv_path)
                    result["upload_result"] = upload_result

                    if upload_result.get("success"):
                        logger.info("Phone tags uploaded to DataSift successfully")
                    else:
                        logger.warning("Phone tag upload issue: %s",
                                       upload_result.get("message"))
                finally:
                    await browser.close()

    result["success"] = True
    result["message"] = (
        f"Phone validation complete: {validation_result['results_count']} scored, "
        f"{validation_result['errors_count']} errors"
    )
    logger.info(result["message"])
    return result


# ── SiftMap: Manage Sold Properties ──────────────────────────────────

DATASIFT_SIFTMAP_URL = "https://app.reisift.io/siftmap"


async def manage_sold_properties(
    page: Page,
    *,
    counties: list[str] | None = None,
    months_back: int = 1,
    min_sale_price: int = 1000,
    sold_tag_date: str | None = None,
) -> dict:
    """Pull recently sold properties from SiftMap and tag them in DataSift.

    Iterates per-county, per-month to tag sold properties with the correct
    sale month. Each month gets "Sold" + "Sold YYYY-MM" tags matching when
    the property actually sold (not the current date).

    Steps per county per month:
    1. Search SiftMap by "Knox County, TN" (county-level search)
    2. Set Last Sold Date filter: first day → last day of that month
    3. Use select-all checkbox + pagination to capture all results
    4. Add to account with "Sold" + "Sold YYYY-MM" tags
    5. Clear filters before next month

    Args:
        page: Logged-in Playwright page.
        counties: Counties to search (default: ["Knox", "Blount"]).
        months_back: How many months back to search for sales (default: 1).
        min_sale_price: Minimum sale price filter to exclude deed transfers.
        sold_tag_date: If set, overrides per-month tag (use for single-month runs).

    Returns:
        Dict with {success, message, counties_processed, total_records, month_details}.
    """
    import calendar as _cal
    from datetime import datetime

    result = {
        "success": False,
        "message": "",
        "counties_processed": [],
        "total_records": 0,
        "month_details": [],
    }

    counties = counties or ["Knox", "Blount"]

    # Build list of (year, month) tuples to process — oldest first
    now = datetime.now()
    months_to_process = []
    for offset in range(months_back, 0, -1):
        # Go back `offset` months from current month
        m = now.month - offset
        y = now.year
        while m <= 0:
            m += 12
            y -= 1
        months_to_process.append((y, m))

    logger.info(
        "Managing sold properties: counties=%s, months=%s, min_price=$%d",
        counties,
        [f"{y}-{m:02d}" for y, m in months_to_process],
        min_sale_price,
    )

    try:
        # Navigate to SiftMap
        logger.info("Navigating to SiftMap...")
        await page.goto(DATASIFT_SIFTMAP_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)
        await _dismiss_popups(page)
        await _screenshot(page, "siftmap_loaded")

        for county in counties:
            county_records = 0
            logger.info("══ Processing %s County ══", county)

            for year, month in months_to_process:
                last_day = _cal.monthrange(year, month)[1]
                start_str = f"{month:02d}/01/{year}"
                end_str = f"{month:02d}/{last_day:02d}/{year}"
                tag_date = sold_tag_date or f"{year}-{month:02d}"

                logger.info(
                    "── %s County %s: %s → %s, tag 'Sold %s' ──",
                    county, f"{year}-{month:02d}", start_str, end_str, tag_date,
                )

                month_result = await _siftmap_search_sold(
                    page,
                    county=county,
                    start_date=start_str,
                    end_date=end_str,
                    min_sale_price=min_sale_price,
                    sold_tag_date=tag_date,
                )

                records = month_result.get("records_added", 0)
                county_records += records
                result["month_details"].append({
                    "county": county,
                    "month": f"{year}-{month:02d}",
                    "records": records,
                    "success": month_result.get("success", False),
                    "message": month_result.get("message", ""),
                })

                if month_result.get("success"):
                    logger.info(
                        "%s %s-%02d: %d records added",
                        county, year, month, records,
                    )
                else:
                    logger.warning(
                        "%s %s-%02d failed: %s",
                        county, year, month,
                        month_result.get("message", "unknown"),
                    )

            result["total_records"] += county_records
            if county_records > 0:
                result["counties_processed"].append(county)
            logger.info(
                "%s County total: %d records across %d months",
                county, county_records, len(months_to_process),
            )

        if result["counties_processed"]:
            result["success"] = True
            result["message"] = (
                f"Sold properties managed for {', '.join(result['counties_processed'])}. "
                f"Total records: {result['total_records']}"
            )
        else:
            result["message"] = "No counties processed successfully"

    except Exception as e:
        result["message"] = f"Manage sold failed: {e}"
        logger.error(result["message"])
        await _screenshot(page, "manage_sold_error")

    return result


async def _siftmap_set_date(page, date_btn_id: str, target_date_str: str, label: str):
    """Set a single date in the SiftMap calendar picker.

    The calendar uses role="dialog" with gridcell day elements, a combobox for
    month name, a spinbutton for year, and "Go to previous/next month" buttons.
    After selecting a day the dialog closes automatically.

    Args:
        page: Playwright page.
        date_btn_id: ID assigned to the date button via JS (e.g., '__siftmap_date_from').
        target_date_str: Date in MM/DD/YYYY format.
        label: Label for logging (e.g., 'start' or 'end').
    """
    import calendar as _cal
    from datetime import datetime as _dt

    target = _dt.strptime(target_date_str, "%m/%d/%Y")
    target_year = target.year
    target_month = target.month
    target_day = target.day
    target_month_name = _cal.month_name[target_month]

    # Click the date button to open the calendar dialog
    date_btn = page.locator(f'#{date_btn_id}')
    await date_btn.click()
    await page.wait_for_timeout(1500)

    cal_dialog = page.locator('[role="dialog"]')
    if await cal_dialog.count() == 0:
        logger.warning("Calendar dialog not found for %s date", label)
        return

    # Set year via spinbutton if different
    year_spin = cal_dialog.locator('[role="spinbutton"], input[type="number"]')
    if await year_spin.count() > 0:
        current_year = await year_spin.first.input_value()
        if str(target_year) != current_year:
            await year_spin.first.click()
            await year_spin.first.press("Control+a")
            await year_spin.first.type(str(target_year), delay=50)
            await year_spin.first.press("Tab")
            await page.wait_for_timeout(500)
            logger.info("Set %s calendar year: %d", label, target_year)

    # Navigate to target month using "Go to previous/next month" buttons
    # The calendar has a combobox showing the current month name
    for _ in range(24):  # max 24 months navigation
        month_combo = cal_dialog.locator('[role="combobox"]')
        if await month_combo.count() == 0:
            # Fallback: read from grid caption (e.g., "February 2026")
            grid = cal_dialog.locator('[role="grid"]')
            if await grid.count() > 0:
                grid_label = await grid.first.get_attribute("aria-label") or ""
                current_month_text = grid_label.split(" ")[0] if grid_label else ""
            else:
                break
        else:
            current_month_text = (await month_combo.first.text_content()).strip()

        if current_month_text == target_month_name:
            break

        current_month_num = list(_cal.month_name).index(current_month_text) if current_month_text in list(_cal.month_name) else 0
        if current_month_num == 0:
            break

        if target_month < current_month_num:
            nav = cal_dialog.get_by_role("button", name="Go to previous month")
            if await nav.count() > 0:
                await nav.first.click()
                await page.wait_for_timeout(400)
        else:
            nav = cal_dialog.get_by_role("button", name="Go to next month")
            if await nav.count() > 0:
                await nav.first.click()
                await page.wait_for_timeout(400)

    logger.info("Navigated %s calendar to %s %d", label, target_month_name, target_year)

    # Click the target day gridcell
    day_cell = cal_dialog.get_by_role("gridcell", name=str(target_day), exact=True)
    if await day_cell.count() > 0:
        await day_cell.first.click()
        await page.wait_for_timeout(1000)
        logger.info("Selected %s day %d via gridcell click", label, target_day)
    else:
        # Fallback: find by text content
        day_btn = cal_dialog.locator(f'td:text-is("{target_day}"), button:text-is("{target_day}")')
        if await day_btn.count() > 0:
            await day_btn.first.click()
            await page.wait_for_timeout(1000)
            logger.info("Selected %s day %d via fallback selector", label, target_day)
        else:
            logger.warning("Could not find %s day %d in calendar", label, target_day)


async def _siftmap_add_page_to_account(
    page, *, county: str, sold_tag_date: str, page_num: int,
) -> dict:
    """Select all filtered properties via "Select Max" and add to account.

    Uses the checkbox dropdown → "Select Max (N)" option to select ALL
    filtered results (not just visible ones), then clicks "Add Records
    to Account". No pagination needed.

    Returns dict with {success, records_added}.
    """
    import re

    # Remove PropertyDetails panel (blocks checkboxes)
    await page.evaluate("""() => {
        document.querySelectorAll('[class*="PropertyDetails"]').forEach(p => p.remove());
    }""")
    await page.wait_for_timeout(500)

    # Remove NPS survey if present
    await page.evaluate("""() => {
        const nps = document.querySelector('[class*="nps"], [class*="NPS"], [class*="survey"]');
        if (nps) nps.remove();
        const els = document.querySelectorAll('*');
        for (const el of els) {
            if (el.textContent.includes('How likely are you to recommend') && el.children.length < 20) {
                el.remove();
                break;
            }
        }
    }""")

    # ── Select all via checkbox dropdown → "Select Max" ──
    # The checkbox dropdown is inside CheckboxDropdownstyles__CheckboxDropdownInnerContainer
    # Clicking it opens a menu: "Select Visible (N)", "Select Max (N)", "Select [custom]"
    select_dropdown = page.locator('[class*="CheckboxDropdown"]').first
    if await select_dropdown.count() > 0:
        await select_dropdown.click()
        await page.wait_for_timeout(1000)

        # Click "Select Max (N)" to select ALL filtered results
        select_max = page.get_by_text(re.compile(r"Select Max"))
        if await select_max.count() > 0:
            max_text = await select_max.first.text_content()
            await select_max.first.click()
            await page.wait_for_timeout(2000)
            logger.info("Clicked '%s'", max_text)
        else:
            # Fallback: try "Select Visible"
            select_visible = page.get_by_text(re.compile(r"Select Visible"))
            if await select_visible.count() > 0:
                vis_text = await select_visible.first.text_content()
                await select_visible.first.click()
                await page.wait_for_timeout(2000)
                logger.info("Clicked '%s' (Select Max not found)", vis_text)
            else:
                logger.warning("No selection options found in dropdown")
    else:
        # Fallback: check individual checkboxes
        logger.warning("Checkbox dropdown not found, trying individual checkboxes")
        await page.evaluate("""() => {
            const allCbs = document.querySelectorAll('input[type="checkbox"]:not([name])');
            for (const cb of allCbs) {
                if (cb.offsetParent !== null && !cb.checked) cb.click();
            }
        }""")
        await page.wait_for_timeout(1000)

    # Verify selection count from "N Properties Selected" text
    selected_text = page.get_by_text(re.compile(r"\d+\s*Properties?\s*Selected", re.IGNORECASE))
    if await selected_text.count() > 0:
        sel_msg = await selected_text.first.text_content()
        logger.info("Selection: %s", sel_msg)
    else:
        logger.warning("Could not verify selection count")

    # Click "Add Records to Account" button (appears after selection)
    add_btn = page.locator('button:has-text("Add Records to Account")')
    add_clicked = False
    if await add_btn.count() > 0:
        await add_btn.first.click(force=True)
        add_clicked = True
    else:
        # Fallback: scroll sidebar to bottom and retry
        await page.evaluate("""() => {
            const containers = document.querySelectorAll(
                '[class*="PropertyList"], [class*="SiftMapContent"]'
            );
            for (const c of containers) {
                if (c.scrollHeight > c.clientHeight) c.scrollTop = c.scrollHeight;
            }
        }""")
        await page.wait_for_timeout(1000)
        if await add_btn.count() > 0:
            await add_btn.first.click(force=True)
            add_clicked = True

    if not add_clicked:
        logger.error("Could not find 'Add Records to Account' button")
        await _screenshot(page, f"siftmap_no_add_btn_{county}")
        return {"success": False, "records_added": 0}

    # Wait for modal (larger selections take longer)
    await page.wait_for_timeout(5000)
    await _screenshot(page, f"siftmap_add_modal_{county}_p{page_num}")

    # Verify modal opened — retry with longer wait if needed
    modal_check = page.get_by_text(re.compile(r"Add [\d,]+ Propert"))
    if await modal_check.count() == 0:
        # Retry: click Add Records button again with longer wait
        logger.warning("Modal not found, retrying Add Records click...")
        add_btn_retry = page.locator('button:has-text("Add Records to Account")')
        if await add_btn_retry.count() > 0:
            await add_btn_retry.first.click(force=True)
            await page.wait_for_timeout(8000)
            modal_check = page.get_by_text(re.compile(r"Add [\d,]+ Propert"))
    if await modal_check.count() == 0:
        logger.error("Page %d: Add Records modal did not open", page_num)
        await _screenshot(page, f"siftmap_no_modal_{county}_p{page_num}")
        return {"success": False, "records_added": 0}

    modal_title = await modal_check.first.text_content()
    logger.info("Page %d modal: %s", page_num, modal_title)
    num_match = re.search(r'([\d,]+)', modal_title or "")
    records_added = int(num_match.group(1).replace(",", "")) if num_match else 0

    # Toggle OFF "Do not replace owners"
    replace_toggle = page.get_by_text(re.compile(r"[Dd]o not replace owners"))
    if await replace_toggle.count() > 0:
        toggle_parent = replace_toggle.first.locator('..')
        toggle_input = toggle_parent.locator(
            'input[type="checkbox"], [class*="toggle"], [class*="switch"]'
        )
        if await toggle_input.count() > 0:
            el_type = await toggle_input.first.get_attribute("type")
            if el_type == "checkbox":
                is_checked = await toggle_input.first.is_checked()
            else:
                toggle_classes = await toggle_input.first.get_attribute("class") or ""
                is_checked = "checked" in toggle_classes
            if is_checked:
                await toggle_input.first.click()
                await page.wait_for_timeout(1000)
                logger.info("Turned OFF 'Do not replace owners' toggle")
        else:
            await replace_toggle.first.click()
            await page.wait_for_timeout(1000)

    # Apply tags
    tags_to_apply = ["Sold", f"Sold {sold_tag_date}"]
    for tag in tags_to_apply:
        try:
            tag_inp = page.locator('input[placeholder*="tag" i], input[name*="tag" i]')
            if await tag_inp.count() == 0:
                tag_inp = page.locator('[class*="tag"] input')
            if await tag_inp.count() == 0:
                continue

            await tag_inp.first.click()
            await tag_inp.first.fill(tag)
            await page.wait_for_timeout(1000)

            # Look for existing tag or "Create" option
            create_opt = page.get_by_text(re.compile(f"[Cc]reate.*{re.escape(tag)}"))
            if await create_opt.count() > 0:
                await create_opt.last.click()
                await page.wait_for_timeout(500)
            else:
                # Try clicking exact match in dropdown first
                exact_opt = page.locator(f'[class*="suggestion"] >> text="{tag}"')
                if await exact_opt.count() > 0:
                    await exact_opt.first.click()
                    await page.wait_for_timeout(500)
                else:
                    await tag_inp.first.press("Enter")
                    await page.wait_for_timeout(500)

            # Dismiss dropdown by clicking modal heading
            modal_heading = page.get_by_text(re.compile(r"Add [\d,]+ Propert"))
            if await modal_heading.count() > 0:
                await modal_heading.first.click()
                await page.wait_for_timeout(300)

            logger.info("Applied tag: %s", tag)
        except Exception as tag_err:
            logger.warning("Failed to apply tag '%s': %s", tag, tag_err)

    await _screenshot(page, f"siftmap_tags_set_{county}_p{page_num}")

    # Confirm
    modal_heading = page.get_by_text(re.compile(r"Add [\d,]+ Propert"))
    if await modal_heading.count() > 0:
        await modal_heading.first.click()
        await page.wait_for_timeout(500)

    await _screenshot(page, f"siftmap_pre_confirm_{county}_p{page_num}")

    confirm_btn = page.locator('button:has-text("Add Properties to Account")')
    if await confirm_btn.count() == 0:
        confirm_btn = page.locator('button:has-text("Add Records")')
    if await confirm_btn.count() == 0:
        confirm_btn = page.locator('button:has-text("Confirm")')

    if await confirm_btn.count() > 0:
        await confirm_btn.first.click(force=True)
        await page.wait_for_timeout(5000)
        logger.info("Confirmed adding %d records", records_added)
    else:
        logger.warning("Could not find confirm button")
        await _screenshot(page, f"siftmap_no_confirm_{county}_p{page_num}")

    await _dismiss_popups(page)
    await _screenshot(page, f"siftmap_complete_{county}_p{page_num}")

    return {"success": True, "records_added": records_added}


async def _siftmap_search_sold(
    page: Page,
    *,
    county: str,
    start_date: str,
    end_date: str,
    min_sale_price: int,
    sold_tag_date: str,
) -> dict:
    """Search SiftMap for sold properties in one county/month and add to account.

    Handles: county search, date range filter, select-all, pagination.

    Args:
        page: Page already on SiftMap.
        county: County name (e.g., "Knox").
        start_date: Start date MM/DD/YYYY (first day of month).
        end_date: End date MM/DD/YYYY (last day of month).
        min_sale_price: Minimum sale price filter.
        sold_tag_date: Tag date string YYYY-MM (matches the sale month).

    Returns:
        Dict with {success, records_added, message}.
    """
    import re
    import json as _json
    from urllib.parse import quote as _quote

    # County FIPS codes for TN counties
    COUNTY_FIPS = {
        "Knox": "47093",
        "Blount": "47009",
    }

    result = {"success": False, "records_added": 0, "message": ""}

    try:
        # ── Step 1: Navigate directly via URL with all filters ──
        # This is far more reliable than interacting with the calendar UI.
        # URL params: location (county JSON), date range, min sale price.
        fips = COUNTY_FIPS.get(county, "47093")

        # Convert dates from MM/DD/YYYY to YYYY-MM-DD for URL params
        from datetime import datetime as _dt
        start_dt = _dt.strptime(start_date, "%m/%d/%Y")
        end_dt = _dt.strptime(end_date, "%m/%d/%Y")
        start_iso = start_dt.strftime("%Y-%m-%d")
        end_iso = end_dt.strftime("%Y-%m-%d")

        location = _json.dumps({
            "searchType": "county",
            "title": f"{county} County, TN",
            "county": county,
            "state": "TN",
            "counties": [{"fips": fips, "county_name": county}],
        })

        url = (
            f"{DATASIFT_SIFTMAP_URL}"
            f"?location={_quote(location)}"
            f"&extra_last_sale_date_min={start_iso}"
            f"&extra_last_sale_date_max={end_iso}"
            f"&extra_last_sale_price_min={min_sale_price}"
        )

        logger.info("SiftMap: Navigating to %s County %s-%s...", county, start_iso, end_iso)
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(8000)
        await _dismiss_popups(page)
        await _screenshot(page, f"siftmap_filtered_{county}")

        # ── Check filtered results count ──
        prop_count = page.get_by_text(re.compile(r"\d+\s*Propert", re.IGNORECASE))
        if await prop_count.count() > 0:
            count_text = await prop_count.first.text_content()
            logger.info("Properties after filtering: %s", count_text)
            num = re.search(r'(\d[\d,]*)', count_text or "")
            if num:
                total_filtered = int(num.group(1).replace(",", ""))
                if total_filtered == 0:
                    result["message"] = f"No sold properties in {county} for {sold_tag_date}"
                    result["success"] = True  # not an error, just no data
                    logger.info(result["message"])
                    return result
                logger.info("Filtered count: %d properties", total_filtered)
        await _screenshot(page, f"siftmap_count_{county}")

        # ── Step 3: Select all and add to account (no pagination needed) ──
        # "Select Max" selects ALL filtered results in one click
        page_result = await _siftmap_add_page_to_account(
            page,
            county=county,
            sold_tag_date=sold_tag_date,
            page_num=1,
        )
        total_records = page_result.get("records_added", 0)

        result["records_added"] = total_records
        result["success"] = True
        result["message"] = (
            f"{county} {sold_tag_date}: {total_records} sold properties added"
        )
        logger.info(result["message"])

        # Navigate back to SiftMap for next month/county
        await page.goto(DATASIFT_SIFTMAP_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)
        await _dismiss_popups(page)

    except Exception as e:
        result["message"] = f"{county} County failed: {e}"
        logger.error(result["message"])
        await _screenshot(page, f"siftmap_error_{county}")

    return result


async def run_manage_sold_workflow(
    *,
    counties: list[str] | None = None,
    months_back: int = 1,
    min_sale_price: int = 1000,
    sold_tag_date: str | None = None,
    email: str | None = None,
    password: str | None = None,
    headless: bool = False,
) -> dict:
    """Full workflow to manage sold properties via SiftMap.

    Top-level orchestrator for the manage-sold CLI command.

    Args:
        counties: Counties to search (default: Knox, Blount).
        months_back: Months of sales to pull (default: 1).
        min_sale_price: Min sale price to exclude deed transfers (default: $1,000).
        sold_tag_date: Tag date YYYY-MM (default: current month).
        email: DataSift login email.
        password: DataSift login password.
        headless: Run browser headless (default False for debugging).

    Returns:
        Dict with workflow results.
    """
    import config as _cfg

    email = email or _cfg.DATASIFT_EMAIL
    password = password or _cfg.DATASIFT_PASSWORD

    if not email or not password:
        return {
            "success": False,
            "message": "DATASIFT_EMAIL and DATASIFT_PASSWORD required",
        }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            logged_in = await login(page, email, password)
            if not logged_in:
                return {"success": False, "message": "DataSift login failed"}

            result = await manage_sold_properties(
                page,
                counties=counties,
                months_back=months_back,
                min_sale_price=min_sale_price,
                sold_tag_date=sold_tag_date,
            )

            # Keep browser open for manual inspection
            logger.info("Browser staying open 30s for inspection...")
            await page.wait_for_timeout(30000)

            return result
        finally:
            await browser.close()


# ════════════════════════════════════════════════════════════════════════════
# Preset Management & Sold Cleanup Sequence  (build 1.0.23+)
# ════════════════════════════════════════════════════════════════════════════

DATASIFT_SEQUENCES_URL = "https://app.reisift.io/sequences"


async def discover_presets(page: Page) -> dict:
    """Discover all filter preset folders and presets, plus existing sequences.

    Navigates to Records → Filter Records to scrape preset folder structure,
    then to Sequences page to list existing sequences.

    Returns:
        {"preset_folders": {"folder_name": ["preset1", ...], ...},
         "sequences": ["seq_name", ...]}
    """
    result = {"preset_folders": {}, "sequences": [], "success": False}

    try:
        # ── Part 1: Discover filter presets ──
        await _navigate_to_records(page)
        await _dismiss_popups(page)
        await _screenshot(page, "discover_records_page")

        # Open filter panel
        filter_link = page.locator('#Records__Filters_Trigger')
        if await filter_link.count() == 0:
            filter_link = page.locator('a:has-text("Filter Records")')

        if await filter_link.count() > 0:
            await filter_link.first.click()
            await page.wait_for_timeout(2000)
            logger.info("Opened filter panel for preset discovery")
        else:
            logger.warning("No Filter Records link found")
            await _screenshot(page, "discover_no_filter_link")
            result["message"] = "Filter Records link not found"
            return result

        await _dismiss_popups(page)
        await _screenshot(page, "discover_filter_panel")

        # Look for presets section — try "Filter Presets", "View Presets", "Load"
        for preset_trigger in ["Filter Presets", "View Presets", "Load", "Saved"]:
            trigger_el = page.get_by_text(preset_trigger, exact=False)
            if await trigger_el.count() > 0:
                await trigger_el.first.click()
                await page.wait_for_timeout(2000)
                logger.info("Clicked '%s' to open presets view", preset_trigger)
                break

        await _screenshot(page, "discover_presets_view")

        # The presets are in a "Filter Presets" section at the BOTTOM of the
        # filter panel (class PresetsBelowstyles).  We need to scroll the
        # filter panel sidebar to make the folders visible.

        # Scroll the "Filter Presets" section into view and scroll down to show folders
        await page.evaluate("""() => {
            // Find the "Filter Presets" text and scroll its parent container
            const allEls = document.querySelectorAll('*');
            for (const el of allEls) {
                if (el.innerText && el.innerText.trim() === 'Filter Presets'
                    && el.getBoundingClientRect().width > 100) {
                    // Found the Filter Presets header — scroll it into view
                    el.scrollIntoView({behavior: 'instant', block: 'start'});
                    // Also try scrolling parent containers
                    let parent = el.parentElement;
                    for (let i = 0; i < 10 && parent; i++) {
                        if (parent.scrollHeight > parent.clientHeight) {
                            parent.scrollTop = parent.scrollHeight;
                        }
                        parent = parent.parentElement;
                    }
                    break;
                }
            }
        }""")
        await page.wait_for_timeout(2000)
        await _screenshot(page, "discover_presets_scrolled")

        # The preset folders are visible in the "Filter Presets" section.
        # They have names like "00 Niche Sequential Marketing".
        # We DON'T need to click/expand them — just scrape the text.
        await page.wait_for_timeout(1000)
        await _screenshot(page, "discover_presets_expanded")

        # Scrape preset folder/preset names using Playwright locators
        # instead of JS evaluate — more reliable for styled-components DOM.
        # We know the exact folder names from the user's screenshot.
        known_folders = [
            "00 Niche Sequential Marketing",
            "00 NICHE SEQUENTIAL MARKETING",
            "01. Bulk Sequential Marketing",
            "01 BULK SEQUENTIAL MARKETING",
            "01. Bulk Sequential",
        ]

        folders_data = {}
        for folder_name_guess in known_folders:
            folder_el = page.get_by_text(folder_name_guess, exact=False)
            if await folder_el.count() > 0:
                # Found this folder — click to expand it
                await folder_el.first.scroll_into_view_if_needed()
                await folder_el.first.click()
                await page.wait_for_timeout(1000)

                # Now scrape all sibling preset items (start with digits)
                presets = await page.evaluate(r"""(folderText) => {
                    const presets = [];
                    // Find the folder header element
                    const allEls = document.querySelectorAll('*');
                    let folderEl = null;
                    for (const el of allEls) {
                        if (el.innerText && el.innerText.trim().indexOf(folderText) !== -1
                            && el.children.length < 5) {
                            folderEl = el;
                            break;
                        }
                    }
                    if (!folderEl) return presets;

                    // Walk siblings and children after the folder header
                    let container = folderEl.parentElement;
                    if (!container) return presets;

                    // Look for links/items within the same container
                    const items = container.querySelectorAll('a, [role="button"], span, div');
                    for (const item of items) {
                        const t = item.innerText ? item.innerText.trim() : '';
                        if (t && /^\d/.test(t) && t.length > 2 && t.length < 60
                            && !presets.includes(t)) {
                            presets.push(t);
                        }
                    }
                    return presets;
                }""", folder_name_guess[:20])

                if presets:
                    folders_data[folder_name_guess] = presets
                    logger.info("Folder '%s': %d presets", folder_name_guess, len(presets))
                    for p in presets:
                        logger.info("  - %s", p)

        # If no known folders matched, try generic discovery via Load button
        if not folders_data:
            load_btn = page.locator('[class*="PresetActions"] >> text="Load"')
            if await load_btn.count() == 0:
                load_btn = page.get_by_text("Load", exact=True)
            if await load_btn.count() > 0:
                await load_btn.first.click()
                await page.wait_for_timeout(2000)
                await _screenshot(page, "discover_load_menu")

                # Scrape everything visible in the load dropdown
                load_items = await page.evaluate(r"""() => {
                    const items = [];
                    const walker = document.createTreeWalker(
                        document.body, NodeFilter.SHOW_TEXT, null, false
                    );
                    let n;
                    let inLoad = false;
                    while (n = walker.nextNode()) {
                        const t = n.textContent.trim();
                        if (t === 'Load') inLoad = true;
                        if (inLoad && t && t.length > 2 && t.length < 80) {
                            items.push(t);
                        }
                    }
                    return items.slice(0, 100);
                }""")
                logger.info("Load menu items: %s", load_items[:30])

        raw_text = folders_data.pop('_raw_text', [])
        if folders_data:
            result["preset_folders"] = folders_data
            for folder, presets in folders_data.items():
                logger.info("Folder '%s': %d presets", folder, len(presets))
                for p in presets:
                    logger.info("  - %s", p)
        else:
            logger.info("No preset folders found in PresetsBelowstyles section")
            if raw_text:
                logger.info("Raw text from presets section:")
                for t in raw_text[:30]:
                    logger.info("  %s", t)

        # Close filter panel
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(1000)

        # ── Part 2: Discover sequences ──
        logger.info("Navigating to Sequences page...")
        await page.goto(DATASIFT_SEQUENCES_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)
        await _dismiss_popups(page)
        await _screenshot(page, "discover_sequences_page")

        # Scrape sequence folder names from the main content area (not sidebar)
        seq_data = await page.evaluate("""() => {
            const folders = [];
            // The sequence folders are in the main content area, not the sidebar.
            // Each folder row has a folder icon + ">" arrow + folder name as an <a> link.
            // The sidebar links are inside elements with class containing "SideBar".
            const links = document.querySelectorAll('a');
            links.forEach(a => {
                const text = a.textContent.trim();
                // Skip sidebar links (they're inside SideBar-styled parents)
                const parent = a.closest('[class*="SideBar"]');
                if (parent) return;
                // Skip header/nav links
                const header = a.closest('[class*="Header"], header, nav');
                if (header) return;
                if (text && text.length > 1 && text.length < 60
                    && text.indexOf('Create') === -1 && text.indexOf('Page') === -1
                    && text.indexOf('Upload') === -1 && text.indexOf('Buy') === -1
                    && text.indexOf('Talk') === -1) {
                    folders.push(text);
                }
            });

            // Also capture all visible text in the main content area for analysis
            const allText = [];
            const main = document.querySelector('main, [class*="content" i], [class*="Content" i]')
                || document.body;
            const walker = document.createTreeWalker(
                main, NodeFilter.SHOW_TEXT, null, false
            );
            let node;
            while (node = walker.nextNode()) {
                const text = node.textContent.trim();
                if (text && text.length > 2 && text.length < 100) {
                    allText.push(text);
                }
            }
            return {folders: [...new Set(folders)].slice(0, 50), allText: [...new Set(allText)].slice(0, 100)};
        }""")

        result["sequences"] = seq_data.get("folders", [])
        result["_sequences_raw_text"] = seq_data.get("allText", [])
        logger.info("Found %d sequence folders", len(result["sequences"]))
        for seq in result["sequences"]:
            logger.info("  Sequence folder: %s", seq)

        if not result["sequences"]:
            logger.info("No sequence folders found — raw text from page:")
            for txt in result.get("_sequences_raw_text", [])[:30]:
                logger.info("  %s", txt)

        result["success"] = True
        result["message"] = (
            f"Found {len(result['preset_folders'])} preset folders, "
            f"{len(result['sequences'])} sequences"
        )
        logger.info(result["message"])

    except Exception as e:
        result["message"] = f"Discovery failed: {e}"
        logger.error(result["message"])
        await _screenshot(page, "discover_error")

    return result


async def _add_sold_status_exclusion(page: Page, preset_name: str) -> dict:
    """Add 'Sold' Property Status exclusion to the currently loaded preset.

    Assumes the filter panel is open and a preset has just been loaded
    (filter blocks are visible in the panel).

    Flow:
      1. Search "Property Status" in the filter block search input
      2. Click to add the Property Status block
      3. Toggle to "do not include" if needed
      4. Select "Sold" status
      5. Click "Save" (overwrites the loaded preset)

    Returns:
        Dict with success status.
    """
    result = {"success": False, "preset": preset_name}

    try:
        await _dismiss_popups(page)

        # ── Add Property Status filter block ──
        logger.info("Adding Property Status exclusion for 'Sold' on '%s'...",
                     preset_name)

        # The search input has placeholder "Add new filter block"
        # Use JS to interact — avoids pointer interception from NPS surveys
        # and overlapping filter block sections.
        added_block = await page.evaluate(r"""() => {
            // Find the "Add new filter block" input
            const inputs = document.querySelectorAll('input');
            let searchInput = null;
            for (const inp of inputs) {
                const ph = (inp.placeholder || '').toLowerCase();
                if (ph.includes('filter block') || ph.includes('add new')) {
                    searchInput = inp;
                    break;
                }
            }
            if (!searchInput) return 'no_search_input';
            searchInput.scrollIntoView({behavior: 'instant', block: 'center'});
            searchInput.focus();
            searchInput.click();
            return 'clicked_search';
        }""")
        logger.info("Search input: %s", added_block)

        if added_block == "clicked_search":
            filter_search = page.locator(
                'input[placeholder*="filter block" i], '
                'input[placeholder*="Add new" i]'
            )
            if await filter_search.count() > 0:
                await filter_search.first.fill("Property Status")
                await page.wait_for_timeout(1500)

                # Click the "Property Status" option from dropdown via JS
                await page.evaluate(r"""() => {
                    const els = document.querySelectorAll('*');
                    for (const el of els) {
                        const t = (el.textContent || '').trim();
                        const rect = el.getBoundingClientRect();
                        if (t === 'Property Status' && rect.height > 0
                            && rect.height < 50 && rect.width > 50
                            && rect.y > 50) {
                            el.click();
                            return true;
                        }
                    }
                    return false;
                }""")
                await page.wait_for_timeout(2000)
                logger.info("Added Property Status filter block")

        await _dismiss_popups(page)
        await _screenshot(page, f"preset_status_block_{preset_name[:15]}")

        # ── Change the new block's dropdown from "Include" to "Do not include" ──
        # The dropdown is a styled component, NOT a native <select>.
        # From the screenshot it shows as a small button with "Include" text
        # and a chevron ∨.  Click it to open, then click "Do not include".
        #
        # Strategy: find the LAST element with text "Include" in the filter
        # panel (x > 700, the newly added Property Status block), click it
        # to open its dropdown, then click "Do not include".
        toggled = await page.evaluate(r"""() => {
            // First try: look for native <select> elements
            const selects = document.querySelectorAll('select');
            for (const sel of [...selects].reverse()) {
                const rect = sel.getBoundingClientRect();
                if (rect.x > 450 && rect.height > 0) {
                    const text = sel.options[sel.selectedIndex]?.text || '';
                    if (text === 'Include') {
                        for (const opt of sel.options) {
                            if (opt.text.toLowerCase().includes('do not')
                                || opt.text.toLowerCase().includes('exclude')) {
                                sel.value = opt.value;
                                const nativeSetter = Object.getOwnPropertyDescriptor(
                                    window.HTMLSelectElement.prototype, 'value'
                                ).set;
                                nativeSetter.call(sel, opt.value);
                                sel.dispatchEvent(new Event('input', {bubbles: true}));
                                sel.dispatchEvent(new Event('change', {bubbles: true}));
                                return {status: 'toggled_native', option: opt.text};
                            }
                        }
                    }
                }
            }
            // Second try: find styled "Include" button/dropdown in the panel
            // Use SelectValue class to find the correct element, and pick
            // the LAST visible one (Property Status block, not Lists/Tags)
            // Use SelectValue class — pick the LAST one with "Include" text
            // (regardless of y position — may be scrolled off screen)
            const selectValues = document.querySelectorAll(
                '[class*="SelectValue"]'
            );
            let lastInclude = null;
            for (const el of selectValues) {
                const t = (el.textContent || '').trim();
                const rect = el.getBoundingClientRect();
                if (t === 'Include' && rect.x > 450 && rect.height > 0) {
                    lastInclude = el;
                }
            }
            if (lastInclude) {
                // Scroll into view first, then click
                lastInclude.scrollIntoView({behavior: 'instant', block: 'center'});
                lastInclude.click();
                const rect = lastInclude.getBoundingClientRect();
                return {status: 'clicked_styled_include',
                        y: Math.round(rect.y)};
            }
            return {status: 'include_not_found'};
        }""")
        logger.info("Toggle result: %s", toggled)

        if toggled.get("status") == "clicked_styled_include":
            # Dropdown should be open — dump what appeared for debugging
            await page.wait_for_timeout(1000)

            # Dump all visible text elements that appeared after clicking
            dropdown_info = await page.evaluate(r"""() => {
                const items = [];
                const allEls = document.querySelectorAll('*');
                for (const el of allEls) {
                    const t = (el.textContent || '').trim();
                    const rect = el.getBoundingClientRect();
                    if (t && t.length > 2 && t.length < 40
                        && rect.height > 0 && rect.height < 50
                        && rect.x > 450 && rect.width < 300
                        && (t.toLowerCase().includes('include')
                            || t.toLowerCase().includes('exclude')
                            || t.toLowerCase().includes('do not'))) {
                        items.push({
                            text: t, tag: el.tagName,
                            x: Math.round(rect.x), y: Math.round(rect.y),
                            w: Math.round(rect.width), h: Math.round(rect.height),
                            cls: (el.className || '').toString().substring(0, 60),
                        });
                    }
                }
                return items;
            }""")
            logger.info("Include dropdown items: %s", dropdown_info)
            await _screenshot(page, f"preset_include_dropdown_{preset_name[:10]}")

            # Try clicking "Do not include" — use the LAST visible one
            # (multiple Select dropdowns exist; we want the Property Status one)
            exclude_clicked = await page.evaluate(r"""() => {
                const allEls = document.querySelectorAll(
                    '[class*="SelectOptionContainer"]'
                );
                let lastDoNotInclude = null;
                for (const el of allEls) {
                    const t = (el.textContent || '').trim();
                    const rect = el.getBoundingClientRect();
                    if (t === 'Do not include' && rect.height > 0
                        && rect.y > 0 && rect.x > 450) {
                        lastDoNotInclude = el;
                    }
                }
                if (lastDoNotInclude) {
                    lastDoNotInclude.click();
                    return {clicked: true, text: 'Do not include',
                            y: Math.round(lastDoNotInclude.getBoundingClientRect().y)};
                }
                // Fallback: any "Do not include" with y > 0
                const allEls2 = document.querySelectorAll('*');
                for (const el of [...allEls2].reverse()) {
                    const t = (el.textContent || '').trim();
                    const rect = el.getBoundingClientRect();
                    if ((t === 'Do not include' || t === 'Do Not Include')
                        && rect.height > 0 && rect.y > 0 && rect.x > 450
                        && el.children.length < 3) {
                        el.click();
                        return {clicked: true, text: t, y: Math.round(rect.y)};
                    }
                }
                return {clicked: false};
            }""")
            logger.info("Exclude click result: %s", exclude_clicked)

            if exclude_clicked.get("clicked"):
                await page.wait_for_timeout(1000)
                logger.info("Selected 'Do not include' from styled dropdown")
            else:
                # The "Include" dropdown may be a toggle — clicking it again
                # may cycle to "Do not include". Try clicking it once more.
                logger.warning("'Do not include' not found — trying toggle click")
                toggle_result = await page.evaluate(r"""() => {
                    const allEls = document.querySelectorAll('*');
                    let lastInclude = null;
                    for (const el of allEls) {
                        const t = (el.textContent || '').trim();
                        const rect = el.getBoundingClientRect();
                        if (t === 'Include' && rect.x > 450 && rect.height > 0
                            && rect.height < 50 && rect.width < 200
                            && el.children.length < 5) {
                            lastInclude = el;
                        }
                    }
                    if (lastInclude) {
                        lastInclude.click();
                        // Check if it changed
                        const newText = lastInclude.textContent.trim();
                        return {clicked: true, newText: newText};
                    }
                    return {clicked: false};
                }""")
                logger.info("Toggle click result: %s", toggle_result)
                await page.wait_for_timeout(1000)
                await _screenshot(page, f"preset_toggle_retry_{preset_name[:10]}")

        await _dismiss_popups(page)

        # ── Type "Sold" in the property status input and select it ──
        sold_added = await page.evaluate(r"""() => {
            // Find the property status input (placeholder "Enter property status")
            const inputs = document.querySelectorAll('input');
            let statusInput = null;
            for (const inp of inputs) {
                const ph = (inp.placeholder || '').toLowerCase();
                if (ph.includes('property status') || ph.includes('status')) {
                    const rect = inp.getBoundingClientRect();
                    if (rect.x > 450 && rect.height > 0) {
                        statusInput = inp;
                    }
                }
            }
            if (!statusInput) return 'no_status_input';
            statusInput.scrollIntoView({behavior: 'instant', block: 'center'});
            statusInput.focus();
            statusInput.click();
            // Set value using native input setter to trigger React state
            const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
                window.HTMLInputElement.prototype, 'value'
            ).set;
            nativeInputValueSetter.call(statusInput, 'Sold');
            statusInput.dispatchEvent(new Event('input', {bubbles: true}));
            statusInput.dispatchEvent(new Event('change', {bubbles: true}));
            return 'typed_sold';
        }""")
        logger.info("Status input: %s", sold_added)
        await page.wait_for_timeout(1500)

        if sold_added == "typed_sold":
            # Select "Sold" from the autocomplete/dropdown
            await _dismiss_popups(page)
            sold_opt = page.get_by_text("Sold", exact=True)
            if await sold_opt.count() > 0:
                for j in range(await sold_opt.count()):
                    box = await sold_opt.nth(j).bounding_box()
                    if box and box["x"] > 450:
                        await sold_opt.nth(j).click(force=True)
                        await page.wait_for_timeout(1000)
                        logger.info("Selected 'Sold' status")
                        break

        await _dismiss_popups(page)
        await _screenshot(page, f"preset_status_sold_{preset_name[:15]}")

        # ── Save/overwrite the preset via JS click ──
        saved = await page.evaluate(r"""() => {
            // Find the "Save" button in the PresetActions bar
            // Buttons: Load | Save | Save New | Clear
            const btns = document.querySelectorAll('[class*="PresetActions"] *');
            for (const btn of btns) {
                const t = (btn.textContent || '').trim();
                if (t === 'Save' && btn.children.length < 3) {
                    btn.click();
                    return 'clicked_save';
                }
            }
            return 'save_not_found';
        }""")
        logger.info("Save result: %s", saved)
        await page.wait_for_timeout(2000)

        # Handle confirmation dialog if it appears
        for confirm_text in ["Overwrite", "Save Preset", "Confirm", "Yes"]:
            confirm_btn = page.get_by_text(confirm_text, exact=False)
            if await confirm_btn.count() > 0:
                await confirm_btn.first.click(force=True)
                await page.wait_for_timeout(1000)
                logger.info("Confirmed: %s", confirm_text)
                break

        await _screenshot(page, f"preset_saved_{preset_name[:20]}")

        result["success"] = True
        result["message"] = f"Updated preset '{preset_name}' with Sold exclusion"
        logger.info(result["message"])

    except Exception as e:
        result["message"] = f"Failed to update preset '{preset_name}': {e}"
        logger.error(result["message"])
        await _screenshot(page, f"preset_error_{preset_name[:20]}")

    return result


async def update_all_presets_sold_exclusion(
    page: Page,
    folders: list[str] | None = None,
) -> dict:
    """Update all presets in target folders to exclude Sold records.

    Opens the Filter Records panel, scrolls to the Filter Presets section,
    expands each folder, scrapes preset names, then loads and updates each.

    Args:
        page: Logged-in Playwright page.
        folders: Folder names to target. Default: Niche + Bulk marketing folders.

    Returns:
        Dict with per-preset results.
    """
    # Default target folders
    if not folders:
        folders = [
            "00 Niche Sequential Marketing",
            "00 NICHE SEQUENTIAL MARKETING",
            "01. Bulk Sequential Marketing",
            "01 BULK SEQUENTIAL MARKETING",
            "01. BULK SEQUENTIAL MARKETING",
        ]

    result = {"success": False, "updated": [], "failed": [], "skipped": []}

    try:
        # ── Navigate to Records and open filter panel ──
        await _navigate_to_records(page)
        await _dismiss_popups(page)

        filter_link = page.locator('#Records__Filters_Trigger')
        if await filter_link.count() == 0:
            filter_link = page.locator('a:has-text("Filter Records")')
        if await filter_link.count() > 0:
            await filter_link.first.click()
            await page.wait_for_timeout(2000)
            logger.info("Opened filter panel")
        else:
            result["message"] = "Filter Records link not found"
            return result

        await _dismiss_popups(page)

        # ── Expand the "Filter Presets" section ──
        # The section header "Filter Presets" is at the bottom of the right
        # panel and may be collapsed.  Click it to expand, then scroll down
        # inside the panel to reveal the folder list.

        # First, click the "Filter Presets" header to expand it
        filter_presets_header = page.get_by_text("Filter Presets", exact=True)
        if await filter_presets_header.count() > 0:
            await filter_presets_header.first.scroll_into_view_if_needed()
            await filter_presets_header.first.click()
            await page.wait_for_timeout(1500)
            logger.info("Clicked 'Filter Presets' header to expand")

        # Scroll the right panel container to the bottom to reveal folders
        await page.evaluate("""() => {
            // Find the filter panel — it's the right-side drawer/panel
            // containing "Filter Records" heading
            const panels = document.querySelectorAll(
                '[class*="FilterPanel"], [class*="Drawer"], [class*="Sidebar"]'
            );
            for (const panel of panels) {
                if (panel.scrollHeight > panel.clientHeight) {
                    panel.scrollTop = panel.scrollHeight;
                }
            }
            // Also try scrolling any container that has the "Filter Presets" text
            const allEls = document.querySelectorAll('*');
            for (const el of allEls) {
                if (el.innerText && el.innerText.includes('Filter Presets')
                    && el.getBoundingClientRect().x > 800) {
                    let parent = el.parentElement;
                    for (let i = 0; i < 15 && parent; i++) {
                        if (parent.scrollHeight > parent.clientHeight + 50) {
                            parent.scrollTop = parent.scrollHeight;
                            break;
                        }
                        parent = parent.parentElement;
                    }
                    break;
                }
            }
        }""")
        await page.wait_for_timeout(2000)
        await _screenshot(page, "presets_section_scrolled")

        # Check if folders are visible — if not, try clicking "View Presets"
        niche_check = page.get_by_text("Niche Sequential", exact=False)
        bulk_check = page.get_by_text("Bulk Sequential", exact=False)
        if await niche_check.count() == 0 and await bulk_check.count() == 0:
            logger.info("Folders not visible yet — trying 'View Presets' link")
            view_presets = page.get_by_text("View Presets", exact=False)
            if await view_presets.count() > 0:
                await view_presets.first.click()
                await page.wait_for_timeout(2000)
                logger.info("Clicked 'View Presets'")

            # Scroll again
            await page.evaluate("""() => {
                const allEls = document.querySelectorAll('*');
                for (const el of allEls) {
                    if (el.innerText && el.innerText.includes('Filter Presets')
                        && el.getBoundingClientRect().x > 800) {
                        let parent = el.parentElement;
                        for (let i = 0; i < 15 && parent; i++) {
                            if (parent.scrollHeight > parent.clientHeight + 50) {
                                parent.scrollTop = parent.scrollHeight;
                                break;
                            }
                            parent = parent.parentElement;
                        }
                        break;
                    }
                }
            }""")
            await page.wait_for_timeout(2000)

        await _screenshot(page, "presets_section_visible")

        # ── Find and expand target folders, scrape presets ──
        folders_data = {}
        for folder_guess in folders:
            folder_el = page.get_by_text(folder_guess, exact=False)
            if await folder_el.count() > 0:
                # Click folder header to expand/toggle
                await folder_el.first.scroll_into_view_if_needed()
                await folder_el.first.click()
                await page.wait_for_timeout(1500)
                logger.info("Clicked folder: %s", folder_guess)

                # Scrape preset names inside this folder.
                # From the screenshot, presets are <a> links like
                # "00. Needs Skipped", "01. Skipped No Numbers" etc.
                # They appear in the Filter Presets panel (x > 500).
                presets = await page.evaluate(r"""(folderText) => {
                    const presets = [];
                    // Find the folder header element by matching text
                    const allEls = [...document.querySelectorAll('*')];
                    let folderEl = null;
                    for (const el of allEls) {
                        const t = (el.textContent || '').trim();
                        if (t.toUpperCase().indexOf(folderText.toUpperCase()) !== -1
                            && el.children.length < 5
                            && el.getBoundingClientRect().width > 100
                            && el.getBoundingClientRect().x > 450) {
                            folderEl = el;
                            break;
                        }
                    }
                    if (!folderEl) return presets;

                    // The preset items are inside the folder's parent container.
                    // Look for PresetTitle elements that are siblings/children
                    // of the folder header's parent.
                    const container = folderEl.closest(
                        '[class*="Collapsible"], [class*="Folder"]'
                    ) || folderEl.parentElement;
                    if (!container) return presets;

                    // Find all preset title elements within this container
                    const titles = container.querySelectorAll(
                        '[class*="PresetTitle"], a, [class*="Preset"]:not([class*="Folder"])'
                    );
                    for (const el of titles) {
                        const t = (el.textContent || '').trim();
                        // Preset names start with two digits + dot
                        if (/^\d{2}\./.test(t) && t.length > 4 && t.length < 80
                            && !presets.includes(t)) {
                            presets.push(t);
                        }
                    }
                    return presets;
                }""", folder_guess[:30])

                if presets:
                    actual_name = folder_guess
                    folders_data[actual_name] = presets
                    logger.info("Folder '%s': %d presets", actual_name, len(presets))
                    for p in presets:
                        logger.info("  - %s", p)
                else:
                    logger.info("No presets found in folder '%s'", folder_guess)

                await _screenshot(page, f"folder_expanded_{folder_guess[:20]}")

        if not folders_data:
            result["message"] = "No presets found in target folders"
            logger.warning(result["message"])
            await _screenshot(page, "no_presets_found")
            return result

        # ── Update each preset ──
        total_presets = sum(len(v) for v in folders_data.values())
        logger.info("Updating %d presets across %d folders",
                     total_presets, len(folders_data))

        for folder_name, presets in folders_data.items():
            logger.info("Processing folder: %s (%d presets)",
                         folder_name, len(presets))

            for preset_name in presets:
                logger.info("Loading preset: %s", preset_name)

                # Click "Clear" first to reset any existing filter blocks
                # Use JS click to avoid viewport issues in the panel
                await page.evaluate("""() => {
                    const els = document.querySelectorAll(
                        '[class*="PresetActions"] *'
                    );
                    for (const el of els) {
                        if ((el.textContent || '').trim() === 'Clear') {
                            el.click();
                            return;
                        }
                    }
                }""")
                await page.wait_for_timeout(1000)

                # Scroll the filter panel to bring this preset into view,
                # then click it.  Playwright's scroll_into_view_if_needed
                # fails here because the scrollable container is the panel,
                # not the viewport.  Use JS to scroll the parent container.
                clicked_preset = await page.evaluate(r"""(presetName) => {
                    const allEls = document.querySelectorAll(
                        '[class*="PresetTitle"], a'
                    );
                    for (const el of allEls) {
                        const t = (el.textContent || '').trim();
                        if (t === presetName && el.getBoundingClientRect().x > 450) {
                            // Scroll the element into view within its panel
                            el.scrollIntoView({behavior: 'instant', block: 'center'});
                            return true;
                        }
                    }
                    return false;
                }""", preset_name)
                await page.wait_for_timeout(500)

                if not clicked_preset:
                    logger.warning("Preset '%s' not found — skipping", preset_name)
                    result["failed"].append(
                        f"{folder_name}/{preset_name}: not found"
                    )
                    continue

                # Now click via JS since the element is inside a scrollable
                # panel that Playwright can't auto-scroll into the viewport.
                loaded = await page.evaluate(r"""(presetName) => {
                    const allEls = document.querySelectorAll(
                        '[class*="PresetTitle"], a'
                    );
                    for (const el of allEls) {
                        const t = (el.textContent || '').trim();
                        if (t === presetName && el.getBoundingClientRect().x > 450) {
                            el.click();
                            return true;
                        }
                    }
                    return false;
                }""", preset_name)

                if loaded:
                    await page.wait_for_timeout(3000)
                    logger.info("Loaded preset: %s", preset_name)
                else:
                    logger.warning("Preset '%s' click failed — skipping", preset_name)
                    result["failed"].append(
                        f"{folder_name}/{preset_name}: click failed"
                    )
                    continue

                await _screenshot(page, f"preset_loaded_{preset_name[:15]}")

                # Add Sold exclusion and save
                update_result = await _add_sold_status_exclusion(
                    page, preset_name
                )
                if update_result.get("success"):
                    result["updated"].append(f"{folder_name}/{preset_name}")
                else:
                    result["failed"].append(
                        f"{folder_name}/{preset_name}: "
                        f"{update_result.get('message')}"
                    )

        result["success"] = len(result["failed"]) == 0
        result["message"] = (
            f"Updated {len(result['updated'])} presets, "
            f"{len(result['failed'])} failed"
        )
        logger.info(result["message"])

    except Exception as e:
        result["message"] = f"Batch update failed: {e}"
        logger.error(result["message"])
        await _screenshot(page, "batch_preset_error")

    return result


async def create_sold_sequence(page: Page) -> dict:
    """Create a 'Sold Property Cleanup' sequence in DataSift.

    UI flow (from screenshot analysis):
      /sequences → "Create New Sequence" →
      Builder page with 3 tabs: Triggers | Conditions | Actions
      Top bar: title input | folder dropdown | Clear | Cancel | Save Sequence

    Triggers are DRAG-AND-DROP from sidebar list to a drop zone.
    After trigger is placed, switch to Conditions tab, then Actions tab.

    Trigger: Property Tags Added → Condition: "Sold" tag
    Actions: Clear Tasks, Remove Lists, Change Status → Sold, Clear Assignee

    Args:
        page: Logged-in Playwright page.

    Returns:
        Dict with success status.
    """
    result = {"success": False, "sequence_name": "Sold Property Cleanup"}

    try:
        # Navigate to Sequences page
        logger.info("Navigating to Sequences page...")
        await page.goto(DATASIFT_SEQUENCES_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)
        await _dismiss_popups(page)
        await _screenshot(page, "sequence_page")

        # Click "Create New Sequence" button
        create_btn = None
        for text in ["Create New Sequence", "Create Sequence", "New Sequence"]:
            btn = page.get_by_text(text, exact=False)
            if await btn.count() > 0:
                create_btn = btn.first
                break

        if not create_btn:
            create_btn_loc = page.locator(
                'button:has-text("Create"), a:has-text("Create")'
            )
            if await create_btn_loc.count() > 0:
                create_btn = create_btn_loc.first

        if create_btn:
            await create_btn.click()
            await page.wait_for_timeout(3000)
            logger.info("Clicked create sequence button")
        else:
            await _screenshot(page, "sequence_no_create_btn")
            result["message"] = "Could not find Create Sequence button"
            return result

        await _dismiss_popups(page)
        await _screenshot(page, "sequence_builder_opened")

        # ── Step 0: Fill title + select folder (top bar) ──
        logger.info("Setting title and folder...")

        title_input = page.locator(
            'input[placeholder*="sequence title" i], '
            'input[placeholder*="title" i], '
            'input[placeholder*="name" i]'
        )
        if await title_input.count() > 0:
            await title_input.first.click()
            await title_input.first.fill("Sold Property Cleanup")
            await page.wait_for_timeout(500)
            logger.info("Set title: Sold Property Cleanup")
        else:
            logger.warning("Title input not found")

        # Folder dropdown — "Select a folder" with chevron
        folder_dropdown = page.locator(
            'select:has(option:has-text("Select a folder")), '
            '[class*="select" i]:has-text("Select a folder")'
        )
        if await folder_dropdown.count() == 0:
            folder_dropdown = page.get_by_text("Select a folder", exact=False)
        if await folder_dropdown.count() > 0:
            await folder_dropdown.first.click()
            await page.wait_for_timeout(1000)
            # Look for Transactions option
            transactions = page.get_by_text("Transactions", exact=True)
            if await transactions.count() > 0:
                await transactions.first.click()
                await page.wait_for_timeout(500)
                logger.info("Selected folder: Transactions")
            else:
                # Try selecting via <option> if it's a native <select>
                try:
                    await page.select_option(
                        'select', label="Transactions"
                    )
                    logger.info("Selected folder via select_option: Transactions")
                except Exception:
                    logger.warning("Could not select Transactions folder")

        await _screenshot(page, "sequence_title_folder")

        # ── Step 1: Drag "Property Tags Added" trigger to drop zone ──
        # The builder uses React DnD — Playwright's drag_to() doesn't fire
        # the right events.  We need to dispatch HTML5 drag events via JS.
        logger.info("Dragging 'Property Tags Added' trigger to drop zone...")

        # First, dump DOM info about the trigger cards to find draggable attrs
        drag_info = await page.evaluate(r"""() => {
            const info = {cards: [], dropZone: null, draggables: []};
            // Find all elements with draggable attribute
            document.querySelectorAll('[draggable="true"]').forEach(el => {
                const rect = el.getBoundingClientRect();
                info.draggables.push({
                    tag: el.tagName,
                    text: (el.innerText || '').substring(0, 50),
                    x: rect.x, y: rect.y, w: rect.width, h: rect.height,
                    classes: String(el.className || '').substring(0, 80),
                });
            });
            // Find trigger card containers
            const allEls = document.querySelectorAll('*');
            for (const el of allEls) {
                const t = (el.innerText || '').trim();
                if (t.startsWith('Property Tags Added') && el.children.length < 5) {
                    const rect = el.getBoundingClientRect();
                    if (rect.width > 100 && rect.width < 500) {
                        info.cards.push({
                            tag: el.tagName,
                            text: t.substring(0, 60),
                            draggable: el.getAttribute('draggable'),
                            x: rect.x, y: rect.y, w: rect.width, h: rect.height,
                            classes: String(el.className || '').substring(0, 100),
                            parentClasses: String(el.parentElement?.className || '').substring(0, 100),
                        });
                    }
                }
            }
            // Find the drop zone
            for (const el of allEls) {
                const t = (el.innerText || '').trim();
                if (t.includes('Drag and drop') && el.children.length < 10) {
                    const rect = el.getBoundingClientRect();
                    if (rect.width > 200) {
                        info.dropZone = {
                            tag: el.tagName,
                            x: rect.x, y: rect.y, w: rect.width, h: rect.height,
                            classes: String(el.className || '').substring(0, 100),
                        };
                        break;
                    }
                }
            }
            return info;
        }""")
        logger.info("Drag info — draggables: %d, cards: %s, dropZone: %s",
                     len(drag_info.get("draggables", [])),
                     drag_info.get("cards", []),
                     drag_info.get("dropZone"))
        for d in drag_info.get("draggables", [])[:10]:
            logger.info("  draggable: %s", d)

        # Mouse-based drag with slow steps — works with React DnD
        async def _mouse_drag(source_el, target_el):
            """Drag source element to target using slow mouse movements."""
            src_box = await source_el.bounding_box()
            dst_box = await target_el.bounding_box()
            if not src_box or not dst_box:
                return False
            sx = src_box["x"] + src_box["width"] / 2
            sy = src_box["y"] + src_box["height"] / 2
            dx = dst_box["x"] + dst_box["width"] / 2
            dy = dst_box["y"] + dst_box["height"] / 2
            await page.mouse.move(sx, sy)
            await page.wait_for_timeout(500)
            await page.mouse.down()
            await page.wait_for_timeout(500)
            steps = 20
            for i in range(1, steps + 1):
                frac = i / steps
                await page.mouse.move(
                    sx + (dx - sx) * frac,
                    sy + (dy - sy) * frac,
                )
                await page.wait_for_timeout(50)
            await page.wait_for_timeout(500)
            await page.mouse.up()
            await page.wait_for_timeout(3000)
            return True

        trigger_el = page.get_by_text("Property Tags Added", exact=True).first
        drop_el = page.get_by_text("Drag and drop a trigger", exact=False).first
        if await trigger_el.count() > 0 and await drop_el.count() > 0:
            if await _mouse_drag(trigger_el, drop_el):
                logger.info("Mouse drag completed — trigger placed")
            else:
                logger.error("Mouse drag failed — no bounding boxes")

        await _dismiss_popups(page)
        await _screenshot(page, "sequence_trigger_placed")

        # Verify trigger was placed
        still_empty = page.get_by_text("Drag and drop a trigger", exact=False)
        if await still_empty.count() > 0:
            logger.error("Trigger not placed — drag failed")
            await _screenshot(page, "sequence_trigger_failed")
            result["message"] = "Could not drag trigger to drop zone"
            return result

        # ── Step 2: Conditions tab → drag "Property Tags" condition ──
        # After trigger placement, page auto-navigates to Conditions tab.
        # Wait for it to settle.
        await page.wait_for_timeout(2000)
        await _dismiss_popups(page)
        await _screenshot(page, "sequence_conditions_tab")

        # Drag "Property Tags" condition card ("Property has certain tags")
        # to the condition drop zone using mouse drag (same technique as trigger)
        logger.info("Dragging 'Property Tags' condition to drop zone...")

        cond_source = page.get_by_text("Property Tags", exact=True).first
        cond_drop = page.get_by_text("Drag and drop a condition", exact=False)

        if await cond_source.count() > 0 and await cond_drop.count() > 0:
            if await _mouse_drag(cond_source, cond_drop.first):
                logger.info("Dragged Property Tags condition to drop zone")
            else:
                logger.warning("Condition drag failed — skipping (optional)")
        else:
            logger.info("Condition drag targets not found — conditions are optional, skipping")

        await _dismiss_popups(page)
        await _screenshot(page, "sequence_condition_placed")

        # If condition was placed, configure it with "Sold" tag
        # The condition card has "Search for tags..." input + "Add" button
        tag_input = page.locator(
            'input[placeholder*="tags" i], '
            'input[placeholder*="Search for" i]'
        )
        for i in range(await tag_input.count()):
            inp = tag_input.nth(i)
            box = await inp.bounding_box()
            if box and box["x"] > 230:  # past sidebar
                await inp.click()
                await inp.fill("Sold")
                await page.wait_for_timeout(1500)
                logger.info("Typed 'Sold' in tag condition input")

                # Look for "Sold" in autocomplete dropdown
                sold_opt = page.get_by_text("Sold", exact=True)
                found_sold = False
                if await sold_opt.count() > 0:
                    for j in range(await sold_opt.count()):
                        opt_box = await sold_opt.nth(j).bounding_box()
                        # Must be below the input (in dropdown) and in main area
                        if opt_box and opt_box["y"] > box["y"] + 20 and opt_box["x"] > 230:
                            await sold_opt.nth(j).click()
                            await page.wait_for_timeout(1000)
                            logger.info("Selected 'Sold' from autocomplete")
                            found_sold = True
                            break

                if not found_sold:
                    # "Sold" not in autocomplete — click "Add" button
                    add_btn = page.get_by_text("Add", exact=True)
                    if await add_btn.count() > 0:
                        await add_btn.last.click()
                        await page.wait_for_timeout(1000)
                        logger.info("Clicked 'Add' to add Sold tag")
                    else:
                        # Try pressing Enter
                        await inp.press("Enter")
                        await page.wait_for_timeout(1000)
                        logger.info("Pressed Enter to add Sold tag")
                break

        # Dismiss any autocomplete dropdowns
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(500)
        # Click on an empty area to dismiss overlays
        await page.mouse.click(800, 100)
        await page.wait_for_timeout(500)

        await _screenshot(page, "sequence_condition_sold")

        # ── Step 3: Navigate to Actions tab ──
        # Tab clicks fail because AdminTabsList intercepts pointer events.
        # Use "Set the Following Actions" button or URL navigation.
        logger.info("Navigating to Actions tab...")

        set_actions_btn = page.get_by_text("Set the Following Actions", exact=False)
        if await set_actions_btn.count() > 0:
            try:
                await set_actions_btn.first.click(timeout=5000)
                await page.wait_for_timeout(3000)
                logger.info("Clicked 'Set the Following Actions' button")
            except Exception:
                # Overlay blocking — try force click
                logger.info("Normal click blocked — trying force click")
                await set_actions_btn.first.click(force=True)
                await page.wait_for_timeout(3000)
                logger.info("Force-clicked 'Set the Following Actions'")
        else:
            # URL-based tab navigation
            await page.goto(
                "https://app.reisift.io/sequences/new/actions",
                wait_until="domcontentloaded",
            )
            await page.wait_for_timeout(3000)
            logger.info("Navigated to Actions tab via URL")

        await _dismiss_popups(page)
        await _screenshot(page, "sequence_actions_tab")

        # Discover what action sidebar cards are available
        action_items = await page.evaluate(r"""() => {
            const items = [];
            // Look for SequenceCreation sidebar cards
            const cards = document.querySelectorAll(
                '[class*="SequenceCreationSideBarCard"]'
            );
            cards.forEach(card => {
                const title = card.querySelector(
                    '[class*="Title"], [class*="CardTitle"]'
                );
                if (title) {
                    items.push(title.innerText.trim());
                } else {
                    const t = card.innerText?.trim();
                    if (t) items.push(t.split('\n')[0]);
                }
            });
            // Fallback: grab all visible text from left sidebar area
            if (items.length === 0) {
                const walker = document.createTreeWalker(
                    document.body, NodeFilter.SHOW_TEXT, null, false
                );
                let n;
                while (n = walker.nextNode()) {
                    const t = n.textContent.trim();
                    if (t && t.length > 3 && t.length < 60) {
                        const rect = n.parentElement?.getBoundingClientRect();
                        if (rect && rect.x > 230 && rect.x < 550 && rect.width > 50) {
                            items.push(t);
                        }
                    }
                }
            }
            return [...new Set(items)].slice(0, 40);
        }""")
        logger.info("Actions sidebar items: %s", action_items)

        # Helper: add an action — first action uses drag, subsequent use
        # "Add new Action +" button which creates a new drop zone.
        actions_added = 0

        async def _add_action(action_text):
            """Add an action by drag (first) or Add new Action + button."""
            nonlocal actions_added

            # For 2nd+ actions, click "Add new Action +" to create a new drop zone
            if actions_added > 0:
                add_btn = page.get_by_text("Add new Action", exact=False)
                if await add_btn.count() > 0:
                    await add_btn.first.scroll_into_view_if_needed()
                    await page.wait_for_timeout(500)
                    try:
                        await add_btn.first.click(timeout=3000)
                    except Exception as e:
                        logger.debug("Normal click failed, forcing: %s", e)
                        await add_btn.first.click(force=True)
                    await page.wait_for_timeout(2000)
                    logger.info("Clicked 'Add new Action +' button")

                    # Scroll the new drop zone into view
                    drop_zone = page.get_by_text("Drag and drop", exact=False)
                    if await drop_zone.count() > 0:
                        await drop_zone.last.scroll_into_view_if_needed()
                        await page.wait_for_timeout(500)

            # Find the sidebar action card
            src = page.get_by_text(action_text, exact=True)
            if await src.count() == 0:
                src = page.get_by_text(action_text, exact=False)
            if await src.count() == 0:
                logger.warning("Action '%s' not found", action_text)
                return False

            # Scroll sidebar card into view before drag
            await src.first.scroll_into_view_if_needed()
            await page.wait_for_timeout(300)

            # Look for drop zone — scroll down if not visible
            drop = page.get_by_text("Drag and drop", exact=False)
            if await drop.count() == 0:
                # Scroll the main content area down to reveal drop zone
                await page.evaluate("""() => {
                    const main = document.querySelector('[class*="SequenceCreation"][class*="Content"]')
                        || document.querySelector('[class*="Content"]')
                        || document.querySelector('main')
                        || document.body;
                    main.scrollTop = main.scrollHeight;
                }""")
                await page.wait_for_timeout(1000)
                drop = page.get_by_text("Drag and drop", exact=False)

            if await drop.count() > 0:
                # Ensure drop zone is visible
                await drop.last.scroll_into_view_if_needed()
                await page.wait_for_timeout(300)
                drag_result = await _mouse_drag(src.first, drop.last)
                if drag_result:
                    actions_added += 1
                return drag_result

            logger.warning("No drop zone found for action '%s'", action_text)
            return False

        # Add actions one at a time (drag each from sidebar to drop zone)
        action_sequence = [
            {
                "search": ["Change Property Status", "Property Status Change",
                           "Property Status"],
                "config": {"type": "select", "value": "Sold"},
            },
            {
                "search": ["Remove Property Lists", "Remove from Lists",
                           "Remove Lists"],
                "config": {
                    "type": "multi_select",
                    "values": ["Pre-Foreclosure", "Probate",
                               "Tax Sale", "Tax Delinquent"],
                },
            },
            {
                "search": ["Clear Property Tasks", "Clear Tasks",
                           "Remove Tasks"],
                "config": None,
            },
            {
                "search": ["Assign Property", "Change Property Assignee",
                           "Property Assignee Change", "Property Assignee"],
                "config": {"type": "clear_assignee"},
            },
        ]

        for idx, action_cfg in enumerate(action_sequence):
            search_texts = action_cfg["search"]
            logger.info("Adding action %d: %s", idx + 1, search_texts[0])

            await _dismiss_popups(page)

            added = False
            for search_text in search_texts:
                if await _add_action(search_text):
                    added = True
                    logger.info("Added action: %s", search_text)
                    break

            if not added:
                logger.warning("Could not add action: %s", search_texts[0])
                continue

            await _dismiss_popups(page)
            await _screenshot(page, f"sequence_action_{idx}_{search_texts[0][:15]}")

            # Configure the action if needed
            config = action_cfg.get("config")
            if config and config["type"] == "select":
                # The action has a styled Select component: "TO" label + dropdown
                # showing "Default" with a chevron. The select container has class
                # Selectstyles__*.  Click the CONTAINER (not just the text) to
                # open the dropdown options.
                val = config["value"]
                await page.wait_for_timeout(1000)

                # Find the styled select container near "CHANGE STATUS TO"
                select_container = page.locator(
                    '[class*="Selectstyles__Select"]:not([class*="Option"])'
                )
                if await select_container.count() > 0:
                    try:
                        await select_container.last.click(timeout=3000)
                    except Exception as e:
                        logger.debug("Normal click failed, forcing: %s", e)
                        await select_container.last.click(force=True)
                    await page.wait_for_timeout(1500)
                    logger.info("  Opened styled select dropdown")
                else:
                    # Fallback: click the chevron arrow inside the dropdown area
                    chevron = page.locator(
                        '[class*="Selectstyles"] svg, '
                        '[class*="Selectstyles"] [class*="Arrow"], '
                        '[class*="Selectstyles"] [class*="Chevron"]'
                    )
                    if await chevron.count() > 0:
                        await chevron.last.click(force=True)
                        await page.wait_for_timeout(1500)
                        logger.info("  Clicked dropdown chevron")
                    else:
                        # Last resort: click "Default" text
                        default_el = page.get_by_text("Default", exact=True)
                        if await default_el.count() > 0:
                            await default_el.last.click(force=True)
                            await page.wait_for_timeout(1500)
                            logger.info("  Clicked Default text")

                await _screenshot(page, f"sequence_action_{idx}_dropdown")

                # Now look for "Sold" in the dropdown options
                sold_opt = page.get_by_text(val, exact=True)
                if await sold_opt.count() > 0:
                    # Find the Sold option that's NOT the condition chip
                    for j in range(await sold_opt.count()):
                        el = sold_opt.nth(j)
                        el_classes = await el.evaluate(
                            "e => String(e.className || '')"
                        )
                        # The dropdown option has "SelectOption" in its class
                        if "SelectOption" in el_classes or "Option" in el_classes:
                            await el.click(force=True)
                            await page.wait_for_timeout(1000)
                            logger.info("  Selected via class match: %s", val)
                            break
                    else:
                        # Fallback: click the last "Sold" that's below y=350
                        for j in range(await sold_opt.count()):
                            el = sold_opt.nth(j)
                            el_box = await el.bounding_box()
                            if el_box and el_box["y"] > 350:
                                await el.click(force=True)
                                await page.wait_for_timeout(1000)
                                logger.info("  Selected via position: %s", val)
                                break

            elif config and config["type"] == "multi_select":
                # The action has a "Search or add a new list" input + "Add"
                # button. Type each value, then click "Add" or select from
                # autocomplete dropdown.
                list_input = page.locator(
                    'input[placeholder*="Search or add" i], '
                    'input[placeholder*="list" i], '
                    'input[placeholder*="search" i]'
                )
                # Find the input that's in the action area (below conditions)
                target_input = None
                for i in range(await list_input.count()):
                    inp = list_input.nth(i)
                    box = await inp.bounding_box()
                    if box and box["x"] > 400 and box["y"] > 300:
                        target_input = inp
                        break

                if target_input:
                    for val in config["values"]:
                        await target_input.click()
                        await target_input.fill(val)
                        await page.wait_for_timeout(1500)

                        # Try to select from autocomplete
                        opt = page.get_by_text(val, exact=True)
                        selected = False
                        if await opt.count() > 0:
                            for j in range(await opt.count()):
                                el_box = await opt.nth(j).bounding_box()
                                t_box = await target_input.bounding_box()
                                # Must be below the input (in dropdown)
                                if el_box and t_box and el_box["y"] > t_box["y"] + 20:
                                    await opt.nth(j).click(force=True)
                                    selected = True
                                    await page.wait_for_timeout(500)
                                    logger.info("  Selected list: %s", val)
                                    break

                        if not selected:
                            # Click "Add" button
                            add_btn = page.get_by_text("Add", exact=True)
                            if await add_btn.count() > 0:
                                # Find the Add button near this input
                                for j in range(await add_btn.count()):
                                    a_box = await add_btn.nth(j).bounding_box()
                                    t_box = await target_input.bounding_box()
                                    if a_box and t_box and abs(a_box["y"] - t_box["y"]) < 30:
                                        await add_btn.nth(j).click(force=True)
                                        await page.wait_for_timeout(500)
                                        logger.info("  Added list: %s", val)
                                        break

                        # Dismiss autocomplete after each selection
                        await target_input.fill("")
                        await page.keyboard.press("Escape")
                        await page.wait_for_timeout(300)

                    # Dismiss any remaining dropdown/overlay after all lists
                    await page.keyboard.press("Escape")
                    await page.mouse.click(800, 100)
                    await page.wait_for_timeout(1000)
                else:
                    logger.warning("  List input not found for multi_select")

            elif config and config["type"] == "clear_assignee":
                # Click "Clear Assignee" button to unassign the property
                clear_btn = page.get_by_text("Clear Assignee", exact=False)
                if await clear_btn.count() > 0:
                    try:
                        await clear_btn.first.click(timeout=3000)
                    except Exception as e:
                        logger.debug("Normal click failed, forcing: %s", e)
                        await clear_btn.first.click(force=True)
                    await page.wait_for_timeout(1000)
                    logger.info("  Clicked Clear Assignee")

            await _screenshot(page, f"sequence_action_{idx}_cfg")

        # ── Step 4: Save sequence ──
        logger.info("Saving sequence...")
        await _dismiss_popups(page)
        await _screenshot(page, "sequence_pre_save")

        save_btn = page.get_by_text("Save Sequence", exact=True)
        if await save_btn.count() == 0:
            save_btn = page.locator(
                'button:has-text("Save Sequence"), '
                'button:has-text("Save"), '
                'a:has-text("Save")'
            )
        if await save_btn.count() > 0:
            try:
                await save_btn.first.click(timeout=5000)
            except Exception as e:
                logger.debug("Normal click failed, forcing: %s", e)
                await save_btn.first.click(force=True)
            await page.wait_for_timeout(3000)
            logger.info("Clicked Save Sequence")

            # Check for duplicate name error and retry with suffix
            dup_error = page.get_by_text("different sequence title", exact=False)
            if await dup_error.count() > 0:
                logger.warning("Duplicate title — retrying with ' V2' suffix")
                title_input = page.locator(
                    'input[placeholder*="sequence title" i], '
                    'input[placeholder*="title" i]'
                )
                if await title_input.count() > 0:
                    await title_input.first.click()
                    await title_input.first.fill("Sold Property Cleanup V2")
                    await page.wait_for_timeout(500)
                    try:
                        await save_btn.first.click(timeout=5000)
                    except Exception as e:
                        logger.debug("Normal click failed, forcing: %s", e)
                        await save_btn.first.click(force=True)
                    await page.wait_for_timeout(3000)
                    result["sequence_name"] = "Sold Property Cleanup V2"
                    logger.info("Saved with title: Sold Property Cleanup V2")
        else:
            logger.warning("Save Sequence button not found")

        await _dismiss_popups(page)
        await _screenshot(page, "sequence_saved")

        # Check URL — should redirect from /new to a sequence detail page
        final_url = page.url
        if "/new" not in final_url or "/sequences/" in final_url:
            logger.info("Sequence saved — URL: %s", final_url)

        result["success"] = True
        result["message"] = "Created 'Sold Property Cleanup' sequence"
        logger.info(result["message"])

    except Exception as e:
        result["message"] = f"Sequence creation failed: {e}"
        logger.error(result["message"])
        await _screenshot(page, "sequence_error")

    return result


async def run_manage_presets_workflow(
    *,
    discover: bool = False,
    add_sold_exclusion: bool = False,
    create_sequence: bool = False,
    preset_folders: list[str] | None = None,
    email: str | None = None,
    password: str | None = None,
    headless: bool = False,
) -> dict:
    """Full workflow for managing filter presets and sequences.

    Top-level orchestrator for the manage-presets CLI command.

    Args:
        discover: Just discover and list presets/sequences.
        add_sold_exclusion: Update presets to exclude Sold records.
        create_sequence: Create the Sold Property Cleanup sequence.
        preset_folders: Folder names to target (default: all).
        email: DataSift login email.
        password: DataSift login password.
        headless: Run browser headless (default False for debugging).

    Returns:
        Dict with workflow results.
    """
    import config as _cfg

    email = email or _cfg.DATASIFT_EMAIL
    password = password or _cfg.DATASIFT_PASSWORD

    if not email or not password:
        return {
            "success": False,
            "message": "DATASIFT_EMAIL and DATASIFT_PASSWORD required",
        }

    result = {"success": False, "discovery": None, "presets": None, "sequence": None}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            logged_in = await login(page, email, password)
            if not logged_in:
                return {"success": False, "message": "DataSift login failed"}

            # Discovery (always run if requested, or if updating presets without folder list)
            if discover or (add_sold_exclusion and not preset_folders):
                logger.info("=== Running preset discovery ===")
                result["discovery"] = await discover_presets(page)

            # Update presets with Sold exclusion
            if add_sold_exclusion:
                logger.info("=== Updating presets with Sold exclusion ===")
                result["presets"] = await update_all_presets_sold_exclusion(
                    page, folders=preset_folders
                )

            # Create Sold cleanup sequence
            if create_sequence:
                logger.info("=== Creating Sold cleanup sequence ===")
                result["sequence"] = await create_sold_sequence(page)

            result["success"] = True
            result["message"] = "Manage presets workflow complete"

            # Keep browser open for manual inspection
            logger.info("Browser staying open 30s for inspection...")
            await page.wait_for_timeout(30000)

            return result
        finally:
            await browser.close()
