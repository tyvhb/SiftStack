"""Entry point for SiftStack — full-stack REI operations platform.

Runs as either:
  - Apify Actor (when APIFY_IS_AT_HOME is set — reads input from Actor.get_input())
  - Standalone CLI (python src/main.py daily --counties Knox --types foreclosure)
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import config
from config import (
    LOG_DIR,
    NOTICE_TYPES,
    OUTPUT_DIR,
    SAVED_SEARCHES,
    SavedSearch,
)
from data_formatter import deduplicate, write_csv, write_csv_by_type
from scraper import scrape_all

logger = logging.getLogger(__name__)


# ── Shared helpers ────────────────────────────────────────────────────


def _filter_searches(
    counties: list[str] | None,
    types: list[str] | None,
) -> list[SavedSearch]:
    """Filter SAVED_SEARCHES by county and/or notice type."""
    searches = list(SAVED_SEARCHES)

    if counties:
        county_set = {c.lower() for c in counties}
        searches = [s for s in searches if s.county.lower() in county_set]

    if types:
        type_set = {t.lower() for t in types}
        searches = [s for s in searches if s.notice_type.lower() in type_set]

    return searches


# ── Preflight health checks ─────────────────────────────────────────


def _preflight_check(mode: str) -> list[str]:
    """Verify required API keys and service connectivity before running.

    Returns a list of failure descriptions. Empty list = all checks passed.
    """
    failures: list[str] = []

    # ── Credential checks (mode-dependent) ──────────────────────────
    scrape_modes = {"daily", "historical"}
    enrichment_modes = scrape_modes | {"pdf-import", "photo-import", "dropbox-watch", "csv-import"}
    datasift_modes = {"manage-presets", "manage-sold", "phone-validate"}

    if mode in scrape_modes:
        if not config.TNPN_EMAIL or not config.TNPN_PASSWORD:
            failures.append("TNPN_EMAIL / TNPN_PASSWORD not set (required for scraping)")
        if not config.CAPTCHA_API_KEY:
            failures.append("CAPTCHA_API_KEY not set (CAPTCHA solving will fail)")

    if mode in enrichment_modes:
        # These are warnings, not blockers — pipeline degrades gracefully
        if not config.SMARTY_AUTH_ID or not config.SMARTY_AUTH_TOKEN:
            logger.warning("Preflight: SMARTY credentials missing — address standardization will be skipped")
        if not config.OPENWEBNINJA_API_KEY:
            logger.warning("Preflight: OPENWEBNINJA_API_KEY missing — Zillow enrichment will be skipped")
        if not config.ANTHROPIC_API_KEY:
            logger.warning("Preflight: ANTHROPIC_API_KEY missing — obituary search and LLM parsing will be skipped")

    if mode in datasift_modes:
        if not config.DATASIFT_EMAIL or not config.DATASIFT_PASSWORD:
            failures.append("DATASIFT_EMAIL / DATASIFT_PASSWORD not set (required for DataSift operations)")

    if mode == "dropbox-watch":
        if not config.DROPBOX_APP_KEY or not config.DROPBOX_APP_SECRET or not config.DROPBOX_REFRESH_TOKEN:
            failures.append("DROPBOX credentials incomplete (need APP_KEY, APP_SECRET, REFRESH_TOKEN)")

    if mode == "phone-validate":
        if not config.TRESTLE_API_KEY:
            failures.append("TRESTLE_API_KEY not set (required for phone validation)")

    # ── Connectivity checks (only for scrape modes) ─────────────────
    if mode in scrape_modes:
        import requests as _requests
        try:
            resp = _requests.head(config.BASE_URL, timeout=10, allow_redirects=True)
            if resp.status_code >= 500:
                failures.append(f"tnpublicnotice.com returned {resp.status_code} — site may be down")
        except Exception as e:
            failures.append(f"Cannot reach tnpublicnotice.com: {e}")

    # ── 2Captcha balance check ──────────────────────────────────────
    if mode in scrape_modes and config.CAPTCHA_API_KEY:
        import requests as _requests
        try:
            resp = _requests.get(
                f"https://2captcha.com/res.php?key={config.CAPTCHA_API_KEY}&action=getbalance",
                timeout=10,
            )
            balance_text = resp.text.strip()
            try:
                balance = float(balance_text)
                if balance < 0.50:
                    failures.append(f"2Captcha balance too low: ${balance:.2f} (need at least $0.50)")
                else:
                    logger.info("Preflight: 2Captcha balance: $%.2f", balance)
            except ValueError:
                if "ERROR" in balance_text:
                    failures.append(f"2Captcha API key invalid: {balance_text}")
        except Exception as e:
            logger.warning("Preflight: Could not check 2Captcha balance: %s", e)

    return failures


# ── Apify Actor mode ─────────────────────────────────────────────────


async def actor_main() -> None:
    """Run as an Apify Actor — full automated pipeline.

    Scrape → Enrich → Tracerfy → DataSift Upload → Slack Notification.
    """
    from apify import Actor
    from time import time as _time

    # Set up Python logging so all modules output at INFO level
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    async with Actor:
        pipeline_start = _time()
        actor_input = await Actor.get_input() or {}

        # Override config credentials from Actor input
        config.TNPN_EMAIL = actor_input.get("tn_username", "")
        config.TNPN_PASSWORD = actor_input.get("tn_password", "")
        config.CAPTCHA_API_KEY = actor_input.get("captcha_api_key", "")
        config.ANTHROPIC_API_KEY = actor_input.get("anthropic_api_key", "")
        config.SMARTY_AUTH_ID = actor_input.get("smarty_auth_id", "")
        config.SMARTY_AUTH_TOKEN = actor_input.get("smarty_auth_token", "")
        config.OPENWEBNINJA_API_KEY = actor_input.get("openwebninja_api_key", "")
        config.SERPER_API_KEY = actor_input.get("serper_api_key", "")
        config.FIRECRAWL_API_KEY = actor_input.get("firecrawl_api_key", "")
        config.TRACERFY_API_KEY = actor_input.get("tracerfy_api_key", "")
        config.DATASIFT_EMAIL = actor_input.get("datasift_email", "")
        config.DATASIFT_PASSWORD = actor_input.get("datasift_password", "")
        config.SLACK_WEBHOOK_URL = actor_input.get("slack_webhook_url", "")
        config.TRESTLE_API_KEY = actor_input.get("trestle_api_key", "")

        mode = actor_input.get("mode", "daily")
        counties = actor_input.get("counties") or None
        types = actor_input.get("types") or None
        since_date_override = actor_input.get("since_date", "").strip()
        start_page = int(actor_input.get("start_page", 1) or 1)
        drive_folder_id = actor_input.get("google_drive_folder_id", "")
        drive_key_b64 = actor_input.get("google_service_account_key", "")

        # Pipeline toggles
        do_upload_datasift = actor_input.get("upload_datasift", True)
        do_enrich_datasift = actor_input.get("enrich_datasift", True)
        do_skip_trace_datasift = actor_input.get("skip_trace_datasift", True)
        do_tracerfy = actor_input.get("run_tracerfy", True)
        do_notify_slack = actor_input.get("notify_slack", True)

        # Buy box / filter toggles
        include_vacant = actor_input.get("include_vacant", False)
        include_commercial = actor_input.get("include_commercial", False)
        include_entities = actor_input.get("include_entities", False)

        # Validate
        if not config.TNPN_EMAIL or not config.TNPN_PASSWORD:
            Actor.log.error("tn_username and tn_password are required")
            try:
                from slack_notifier import notify_preflight_failure
                notify_preflight_failure(["TNPN credentials missing"])
            except Exception:
                pass
            await Actor.fail(status_message="Missing SiftStack credentials")
            return
        if not config.CAPTCHA_API_KEY:
            Actor.log.warning("captcha_api_key not set — CAPTCHA solving will fail")

        # Filter searches
        searches = _filter_searches(counties, types)
        if not searches:
            Actor.log.error("No saved searches match the given counties/types filters")
            await Actor.fail(status_message="No matching saved searches")
            return

        Actor.log.info(
            "Running %d saved searches: %s",
            len(searches),
            ", ".join(s.saved_search_name for s in searches),
        )

        # Set up residential proxy if requested
        proxy_url: str | None = None
        use_proxy = actor_input.get("use_residential_proxy", True)
        if use_proxy:
            try:
                proxy_config = await Actor.create_proxy_configuration(
                    groups=["RESIDENTIAL"]
                )
                proxy_url = await proxy_config.new_url()
                Actor.log.info("Residential proxy configured")
            except Exception:
                Actor.log.warning("Could not configure residential proxy — running without proxy")

        # Track seen notice IDs for incremental dedup
        seen_ids: set[str] = set()

        def _notice_id(url: str) -> str:
            import re
            m = re.search(r"[?&]ID=(\d+)", url)
            return m.group(1) if m else ""

        async def push_batch(batch_notices):
            """Push new unique notices to dataset immediately after each search."""
            unique = []
            for n in batch_notices:
                nid = _notice_id(n.source_url)
                if nid and nid in seen_ids:
                    continue
                if nid:
                    seen_ids.add(nid)
                unique.append(n)
            if unique:
                await Actor.push_data([
                    {
                        "date_added": n.date_added,
                        "address": n.address,
                        "city": n.city,
                        "state": n.state,
                        "zip": n.zip,
                        "owner_name": n.owner_name,
                        "notice_type": n.notice_type,
                        "county": n.county,
                        "decedent_name": n.decedent_name,
                        "owner_street": n.owner_street,
                        "owner_city": n.owner_city,
                        "owner_state": n.owner_state,
                        "owner_zip": n.owner_zip,
                        "auction_date": n.auction_date,
                        "zip_plus4": n.zip_plus4,
                        "latitude": n.latitude,
                        "longitude": n.longitude,
                        "dpv_match_code": n.dpv_match_code,
                        "vacant": n.vacant,
                        "rdi": n.rdi,
                        "mls_status": n.mls_status,
                        "mls_listing_price": n.mls_listing_price,
                        "mls_last_sold_date": n.mls_last_sold_date,
                        "mls_last_sold_price": n.mls_last_sold_price,
                        "estimated_value": n.estimated_value,
                        "estimated_equity": n.estimated_equity,
                        "equity_percent": n.equity_percent,
                        "property_type": n.property_type,
                        "bedrooms": n.bedrooms,
                        "bathrooms": n.bathrooms,
                        "sqft": n.sqft,
                        "year_built": n.year_built,
                        "lot_size": n.lot_size,
                        "source_url": n.source_url,
                        "raw_text": n.raw_text[:5000] if n.raw_text else "",
                    }
                    for n in unique
                ])
                Actor.log.info("Pushed %d records to dataset (incremental)", len(unique))

        # Log LLM parser status
        if config.ANTHROPIC_API_KEY:
            Actor.log.info("LLM fallback enabled (Claude Haiku) for missing fields")
        else:
            Actor.log.info("LLM fallback disabled — set anthropic_api_key to enable")

        if start_page > 1:
            Actor.log.info("Starting from page %d (skipping earlier pages)", start_page)

        try:
            # ── Load last_run_date from Apify KVS (persists between runs) ──
            if mode == "daily" and not since_date_override:
                kvs = await Actor.open_key_value_store()
                stored = await kvs.get_value("last_run_date")
                if stored:
                    since_date_override = stored
                    Actor.log.info("Daily mode: using stored last_run_date = %s", stored)
                else:
                    Actor.log.info("Daily mode: no stored last_run_date, defaulting to 7 days")

            # ── Scrape ────────────────────────────────────────────────
            notices = await scrape_all(
                mode=mode, searches=searches, proxy_url=proxy_url, on_batch=push_batch,
                since_date_override=since_date_override or None,
                llm_api_key=config.ANTHROPIC_API_KEY or None,
                start_page=start_page,
            )
            # Handle async probate lookup before pipeline (requires await)
            probate_notices = [n for n in notices if n.notice_type == "probate" and n.decedent_name and not n.address]
            if probate_notices:
                try:
                    from property_lookup import lookup_decedent_properties
                    Actor.log.info("Looking up property addresses for %d probate notices...", len(probate_notices))
                    await lookup_decedent_properties(probate_notices)
                except ImportError:
                    Actor.log.warning("property_lookup module not found -- skipping property lookup")
                except Exception as e:
                    Actor.log.warning("Property lookup failed: %s -- continuing without lookups", e)

            # ── Enrichment ────────────────────────────────────────────
            from enrichment_pipeline import PipelineOptions, run_enrichment_pipeline

            opts = PipelineOptions(
                skip_parcel_lookup=True,  # web scrape notices don't have parcel IDs
                skip_vacant_filter=include_vacant,
                skip_commercial_filter=include_commercial,
                skip_entity_filter=include_entities,
                source_label="Apify Actor",
            )
            notices = run_enrichment_pipeline(notices, opts)

            if not notices:
                Actor.log.warning("No notices found")
                return

            total = len(notices)

            # ── Tracerfy Skip Trace ───────────────────────────────────
            tracerfy_stats = None
            if do_tracerfy and config.TRACERFY_API_KEY:
                Actor.log.info("Running Tracerfy batch skip trace...")
                try:
                    from tracerfy_skip_tracer import batch_skip_trace
                    tracerfy_stats = batch_skip_trace(notices)
                    Actor.log.info(
                        "Tracerfy: %d/%d matched, %d phones, %d emails, $%.2f",
                        tracerfy_stats["matched"], tracerfy_stats["submitted"],
                        tracerfy_stats["phones_found"], tracerfy_stats["emails_found"],
                        tracerfy_stats["cost"],
                    )
                except Exception as e:
                    Actor.log.warning("Tracerfy skip trace failed: %s — continuing", e)
            elif do_tracerfy:
                Actor.log.info("Tracerfy skipped — no API key configured")

            # ── Write CSV ─────────────────────────────────────────────
            csv_path = write_csv(notices)
            kvs = await Actor.open_key_value_store()
            with open(csv_path, "rb") as f:
                await kvs.set_value("output.csv", f.read(), content_type="text/csv")
            Actor.log.info("CSV saved to key-value store as 'output.csv'")

            # ── Google Drive Upload ───────────────────────────────────
            if drive_folder_id and drive_key_b64:
                Actor.log.info("Uploading to Google Drive...")
                from drive_uploader import upload_csv, upload_summary

                by_type: dict[str, int] = {}
                by_county: dict[str, int] = {}
                for n in notices:
                    by_type[n.notice_type] = by_type.get(n.notice_type, 0) + 1
                    by_county[n.county] = by_county.get(n.county, 0) + 1

                file_id = upload_csv(csv_path, drive_folder_id, drive_key_b64, total)
                if file_id:
                    Actor.log.info("CSV uploaded to Drive (file ID: %s)", file_id)
                else:
                    Actor.log.error("CSV upload to Drive failed — CSV still in key-value store")

                upload_summary(by_type, by_county, total, drive_folder_id, drive_key_b64)
            elif drive_folder_id:
                Actor.log.warning("google_drive_folder_id set but google_service_account_key missing — skipping Drive upload")

            # ── DataSift Upload ───────────────────────────────────────
            upload_result = None
            if do_upload_datasift and config.DATASIFT_EMAIL and config.DATASIFT_PASSWORD:
                Actor.log.info("Uploading to DataSift.ai...")
                try:
                    from datasift_formatter import write_datasift_split_csvs
                    from datasift_uploader import upload_datasift_split, upload_to_datasift

                    csv_infos = write_datasift_split_csvs(notices)
                    for info in csv_infos:
                        Actor.log.info("DataSift CSV (%s): %s", info["label"], info["path"])

                    if len(csv_infos) > 1:
                        upload_result = await upload_datasift_split(
                            csv_infos,
                            enrich=do_enrich_datasift,
                            skip_trace=do_skip_trace_datasift,
                        )
                    else:
                        upload_result = await upload_to_datasift(
                            csv_infos[0]["path"],
                            enrich=do_enrich_datasift,
                            skip_trace=do_skip_trace_datasift,
                        )

                    if upload_result.get("success"):
                        Actor.log.info("DataSift upload: %s", upload_result.get("message", "OK"))
                    else:
                        Actor.log.error("DataSift upload failed: %s", upload_result.get("message"))
                except Exception as e:
                    Actor.log.error("DataSift upload error: %s", e)
                    upload_result = {"success": False, "message": str(e)}
            elif do_upload_datasift:
                Actor.log.info("DataSift upload skipped — no credentials configured")

            # ── Slack Notification ────────────────────────────────────
            elapsed_min = (_time() - pipeline_start) / 60
            if do_notify_slack and config.SLACK_WEBHOOK_URL:
                try:
                    from slack_notifier import send_slack_notification
                    send_slack_notification(
                        notices,
                        upload_result=upload_result,
                        elapsed_min=elapsed_min,
                    )
                    Actor.log.info("Slack notification sent")
                except Exception as e:
                    Actor.log.warning("Slack notification failed: %s", e)

            # ── Save last_run_date to Apify KVS for next run ─────
            from datetime import datetime as _dt
            kvs = await Actor.open_key_value_store()
            await kvs.set_value("last_run_date", _dt.now().strftime("%Y-%m-%d"))
            Actor.log.info("Saved last_run_date to KVS for next daily run")

            Actor.log.info("Done — %d notices exported (%.1f min)", total, elapsed_min)

        except Exception as e:
            Actor.log.error("Pipeline failed: %s", e, exc_info=True)
            try:
                from slack_notifier import notify_error
                notify_error("Apify Actor Pipeline", e, context=f"mode={mode}")
            except Exception:
                pass
            await Actor.fail(status_message=f"Pipeline error: {e}")


# ── CLI mode ──────────────────────────────────────────────────────────


def setup_logging(verbose: bool = False) -> None:
    """Configure logging to both console and date-stamped log file."""
    level = logging.DEBUG if verbose else logging.INFO
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_file = LOG_DIR / f"scrape_{timestamp}.log"

    # Force UTF-8 on console output to avoid cp1252 encoding errors on Windows
    console = logging.StreamHandler(
        open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)
    )
    handlers: list[logging.Handler] = [
        console,
        logging.FileHandler(log_file, encoding="utf-8"),
    ]

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )
    logging.info("Logging to %s", log_file)


def _run_pdf_import(args) -> None:
    """Run the PDF import pipeline: OCR → parse → enrich → CSV."""
    from pdf_importer import process_pdf
    from enrichment_pipeline import PipelineOptions, run_enrichment_pipeline

    # Validate required args
    if not args.pdf_path:
        logging.error("--pdf-path is required for pdf-import mode")
        sys.exit(1)
    if not args.pdf_county:
        logging.error("--pdf-county is required for pdf-import mode")
        sys.exit(1)

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        logging.error("PDF file not found: %s", pdf_path)
        sys.exit(1)

    county = args.pdf_county.strip().title()  # "knox" → "Knox"

    api_key = config.ANTHROPIC_API_KEY or None

    # OCR + parse
    notices = process_pdf(
        pdf_path=pdf_path,
        county=county,
        api_key=api_key,
        date_added=args.pdf_date,
        regex_only=args.regex_only,
    )

    if not notices:
        logging.warning("No records extracted from PDF")
        sys.exit(0)

    # Run unified enrichment pipeline
    opts = PipelineOptions(
        skip_parcel_lookup=args.skip_tax,
        skip_smarty=args.skip_smarty,
        skip_zillow=args.skip_zillow,
        skip_tax=args.skip_tax,
        skip_geocode=getattr(args, "skip_geocode", False),
        skip_obituary=args.skip_obituary,
        skip_ancestry=getattr(args, "skip_ancestry", False),
        skip_entity_research=not getattr(args, "research_entities", False),
        skip_vacant_filter=getattr(args, "include_vacant", False),
        skip_commercial_filter=getattr(args, "include_commercial", False),
        skip_entity_filter=getattr(args, "include_entities", False),
        skip_heir_verification=args.skip_heir_verification,
        max_heir_depth=args.max_heir_depth,
        skip_dm_address=args.skip_dm_address,
        tracerfy_tier1=getattr(args, "tracerfy_tier1", False),
        source_label=f"PDF import ({pdf_path.name})",
    )
    notices = run_enrichment_pipeline(notices, opts)

    if not notices:
        logging.warning("No records remaining after pipeline")
        return

    # Write output
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"{county.lower()}_tax_sale_{timestamp}.csv"
    path = write_csv(notices, filename=filename)
    logging.info("Output: %s", path)
    logging.info("Done — %d records exported", len(notices))


def _run_photo_import(args) -> None:
    """Run the photo import pipeline: preprocess → OCR → parse → enrich → CSV."""
    from photo_importer import process_photos
    from enrichment_pipeline import PipelineOptions, run_enrichment_pipeline

    # Validate required args
    if not args.folder:
        logging.error("--folder is required for photo-import mode")
        sys.exit(1)
    if not args.photo_county:
        logging.error("--photo-county is required for photo-import mode")
        sys.exit(1)
    if not args.photo_type:
        logging.error("--photo-type is required for photo-import mode")
        sys.exit(1)

    folder = Path(args.folder)
    if not folder.exists() or not folder.is_dir():
        logging.error("Folder not found: %s", folder)
        sys.exit(1)

    county = args.photo_county.strip().title()

    notice_type = args.photo_type.strip().lower()
    api_key = config.ANTHROPIC_API_KEY or None

    # OCR + parse
    notices = process_photos(
        folder=folder,
        county=county,
        notice_type=notice_type,
        date_added=args.photo_date,
        api_key=api_key,
        correct_perspective=not getattr(args, "no_perspective_correct", False),
    )

    if not notices:
        logging.warning("No records extracted from photos")
        sys.exit(0)

    # Run unified enrichment pipeline
    # Skip vacant land filter for notice types without property addresses
    # (probate from court terminals never has property address — would filter everything)
    no_address_types = {"probate", "divorce"}
    opts = PipelineOptions(
        skip_vacant_filter=getattr(args, "include_vacant", False) or notice_type in no_address_types,
        skip_commercial_filter=getattr(args, "include_commercial", False),
        skip_entity_filter=getattr(args, "include_entities", False),
        skip_parcel_lookup=args.skip_tax,
        skip_smarty=args.skip_smarty,
        skip_zillow=args.skip_zillow,
        skip_tax=args.skip_tax,
        skip_geocode=getattr(args, "skip_geocode", False),
        skip_obituary=args.skip_obituary,
        skip_ancestry=getattr(args, "skip_ancestry", False),
        skip_entity_research=not getattr(args, "research_entities", False),
        skip_heir_verification=args.skip_heir_verification,
        max_heir_depth=args.max_heir_depth,
        skip_dm_address=args.skip_dm_address,
        tracerfy_tier1=getattr(args, "tracerfy_tier1", False),
        source_label=f"Photo import ({folder.name})",
    )
    notices = run_enrichment_pipeline(notices, opts)

    if not notices:
        logging.warning("No records remaining after pipeline")
        return

    # Write output
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"{county.lower()}_{notice_type}_{timestamp}.csv"
    path = write_csv(notices, filename=filename)
    logging.info("Output: %s", path)
    logging.info("Done — %d records exported", len(notices))


def _run_csv_import(args) -> None:
    """Run the CSV re-import pipeline: read CSV → enrich → write new CSV.

    Supports multiple CSV paths (comma-separated) for merging datasets.
    Supports --upload-datasift to format and upload to DataSift after enrichment.
    """
    from data_formatter import read_csv
    from enrichment_pipeline import (
        PipelineOptions,
        detect_existing_enrichment,
        run_enrichment_pipeline,
    )

    # Validate required args
    if not args.csv_path:
        logging.error("--csv-path is required for csv-import mode")
        sys.exit(1)

    # Support multiple CSV paths (comma-separated)
    csv_paths = [Path(p.strip()) for p in args.csv_path.split(",")]
    for cp in csv_paths:
        if not cp.exists():
            logging.error("CSV file not found: %s", cp)
            sys.exit(1)

    county = None
    if args.csv_county:
        county = args.csv_county.strip().title()

    # Read all CSVs → NoticeData, merge
    all_notices = []
    for cp in csv_paths:
        batch = read_csv(cp)
        logging.info("Loaded %d records from %s", len(batch), cp.name)
        all_notices.extend(batch)

    if not all_notices:
        logging.warning("No records found in CSV(s)")
        sys.exit(0)

    # Deduplicate by source_url (notice ID) — keeps most recent
    seen_urls = {}
    for n in all_notices:
        url = getattr(n, "source_url", "") or ""
        if url and url in seen_urls:
            # Keep the one with more enrichment data
            existing = seen_urls[url]
            if (getattr(n, "estimated_value", "") or "") and not (getattr(existing, "estimated_value", "") or ""):
                seen_urls[url] = n
        elif url:
            seen_urls[url] = n
        else:
            # No source_url — keep all (dedup by address later)
            seen_urls[id(n)] = n
    notices = list(seen_urls.values())
    if len(notices) < len(all_notices):
        logging.info("Deduped %d → %d records (by source_url)", len(all_notices), len(notices))

    # Override county if provided (for CSVs without county column)
    if county:
        for n in notices:
            if not n.county.strip():
                n.county = county

    logging.info("Total: %d records from %d CSV(s)", len(notices), len(csv_paths))

    # Build pipeline options
    primary_name = csv_paths[0].name
    opts = PipelineOptions(
        skip_filter_sold=False,
        skip_vacant_filter=getattr(args, "include_vacant", False),
        skip_commercial_filter=getattr(args, "include_commercial", False),
        skip_entity_filter=getattr(args, "include_entities", False),
        skip_smarty=args.skip_smarty,
        skip_zillow=args.skip_zillow,
        skip_tax=args.skip_tax,
        skip_geocode=getattr(args, "skip_geocode", False),
        skip_obituary=args.skip_obituary,
        skip_ancestry=getattr(args, "skip_ancestry", False),
        skip_entity_research=not getattr(args, "research_entities", False),
        skip_heir_verification=args.skip_heir_verification,
        max_heir_depth=args.max_heir_depth,
        skip_dm_address=args.skip_dm_address,
        tracerfy_tier1=getattr(args, "tracerfy_tier1", False),
        source_label=f"CSV import ({primary_name})",
    )
    detect_existing_enrichment(notices, opts)
    notices = run_enrichment_pipeline(notices, opts)

    if not notices:
        logging.warning("No records remaining after pipeline")
        return

    # Write output
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"{csv_paths[0].stem}_reimport_{timestamp}.csv"
    path = write_csv(notices, filename=filename)
    logging.info("Output: %s", path)

    # DataSift upload (same logic as daily/historical mode)
    if getattr(args, "upload_datasift", False):
        from datasift_formatter import write_datasift_split_csvs
        from datasift_uploader import upload_datasift_split, upload_to_datasift

        do_enrich = not getattr(args, "no_enrich", False)
        do_skip_trace = not getattr(args, "no_skip_trace", False)

        csv_infos = write_datasift_split_csvs(notices)
        for info in csv_infos:
            logging.info("DataSift CSV (%s): %s", info["label"], info["path"])

        if len(csv_infos) > 1:
            upload_result = asyncio.run(
                upload_datasift_split(
                    csv_infos,
                    enrich=do_enrich,
                    skip_trace=do_skip_trace,
                )
            )
        else:
            upload_result = asyncio.run(
                upload_to_datasift(
                    csv_infos[0]["path"],
                    enrich=do_enrich,
                    skip_trace=do_skip_trace,
                )
            )

        if upload_result.get("success"):
            logging.info("DataSift upload: %s", upload_result.get("message", "OK"))
        else:
            logging.error("DataSift upload failed: %s", upload_result.get("message"))

    logging.info("Done — %d records exported", len(notices))


def _run_phone_validate(args) -> None:
    """Run phone validation via Trestle API with DataSift export/upload."""
    import json as _json

    csv_path = getattr(args, "csv_path", None)
    list_name = getattr(args, "list_name", None)
    preset_folder = getattr(args, "preset_folder", None)
    all_records = getattr(args, "all_records", False)

    # Must specify at least one targeting mode
    if not csv_path and not list_name and not preset_folder and not all_records:
        logging.error(
            "phone-validate requires one of: --csv-path, --list-name, --preset-folder, or --all-records"
        )
        sys.exit(1)

    # Parse custom tiers if provided
    tiers = None
    custom_tiers_str = getattr(args, "custom_tiers", None)
    if custom_tiers_str:
        try:
            raw = _json.loads(custom_tiers_str)
            tiers = {k: tuple(v) for k, v in raw.items()}
            logging.info("Using custom tiers: %s", tiers)
        except (_json.JSONDecodeError, ValueError) as e:
            logging.error("Invalid --custom-tiers JSON: %s", e)
            sys.exit(1)

    # Estimate-only mode
    if getattr(args, "estimate", False):
        from phone_validator import estimate_cost, print_estimate

        if csv_path:
            est = estimate_cost(csv_path)
            print_estimate(est)
        else:
            logging.error("--estimate requires --csv-path (export from DataSift first, then estimate)")
            sys.exit(1)
        return

    # Full validation workflow
    from datasift_uploader import run_phone_validation_workflow

    result = asyncio.run(run_phone_validation_workflow(
        list_name=list_name,
        preset_folder=preset_folder,
        all_records=all_records,
        csv_path=csv_path,
        upload_tags=not getattr(args, "no_upload", False),
        api_key=config.TRESTLE_API_KEY or None,
        tiers=tiers,
        add_litigator=getattr(args, "add_litigator", False),
        batch_size=getattr(args, "batch_size", 10),
    ))

    if result.get("success"):
        logging.info("Phone validation: %s", result.get("message", "OK"))
        if result.get("validation_result"):
            vr = result["validation_result"]
            logging.info("  Results: %d scored, %d errors", vr.get("results_count", 0), vr.get("errors_count", 0))
            for tag, count in vr.get("tier_counts", {}).items():
                logging.info("    %s: %d", tag, count)
        if result.get("upload_result"):
            logging.info("  Tag upload: %s", result["upload_result"].get("message", ""))
    else:
        logging.error("Phone validation failed: %s", result.get("message"))
        sys.exit(1)


def _run_manage_presets(args) -> None:
    """Run the DataSift filter preset management workflow."""
    from datasift_uploader import run_manage_presets_workflow

    discover = getattr(args, "discover", False)
    add_sold = getattr(args, "add_sold_exclusion", False)
    create_seq = getattr(args, "create_sold_sequence", False)

    # Default to discover if no flags specified
    if not (discover or add_sold or create_seq):
        discover = True

    preset_folders = None
    if getattr(args, "preset_folders", None):
        preset_folders = [f.strip() for f in args.preset_folders.split(",")]

    result = asyncio.run(run_manage_presets_workflow(
        discover=discover,
        add_sold_exclusion=add_sold,
        create_sequence=create_seq,
        preset_folders=preset_folders,
    ))

    if result.get("success"):
        logging.info("Manage presets: %s", result.get("message", "OK"))
        if result.get("discovery"):
            disc = result["discovery"]
            for folder, presets in disc.get("preset_folders", {}).items():
                logging.info("  Folder '%s': %s", folder, presets)
            logging.info("  Sequences: %s", disc.get("sequences", []))
        if result.get("presets"):
            p = result["presets"]
            logging.info("  Updated: %s", p.get("updated", []))
            logging.info("  Failed: %s", p.get("failed", []))
        if result.get("sequence"):
            logging.info("  Sequence: %s", result["sequence"].get("message"))
    else:
        logging.error("Manage presets failed: %s", result.get("message"))
        sys.exit(1)


def _run_manage_sold(args) -> None:
    """Run the SiftMap sold properties management workflow."""
    from datasift_uploader import run_manage_sold_workflow

    # Parse counties if provided, otherwise use default (Knox, Blount)
    counties = None
    if args.counties and args.counties.lower() != "all":
        counties = [c.strip().title() for c in args.counties.split(",")]

    result = asyncio.run(run_manage_sold_workflow(
        counties=counties,
        months_back=getattr(args, "months_back", 1),
        min_sale_price=getattr(args, "min_sale_price", 1000),
        sold_tag_date=getattr(args, "sold_tag_date", None),
    ))

    if result.get("success"):
        logging.info("Manage sold: %s", result.get("message", "OK"))
        logging.info("  Counties: %s", ", ".join(result.get("counties_processed", [])))
        logging.info("  Total records: %d", result.get("total_records", 0))
    else:
        logging.error("Manage sold failed: %s", result.get("message"))
        sys.exit(1)


def cli_main() -> None:
    """Run as standalone CLI."""
    parser = argparse.ArgumentParser(
        description="SiftStack — full-stack REI operations platform"
    )
    parser.add_argument(
        "mode",
        choices=[
            "daily", "historical", "pdf-import", "photo-import", "dropbox-watch",
            "csv-import", "phone-validate", "manage-sold", "manage-presets",
            # New analysis & workflow modes
            "comp", "rehab", "analyze-deal", "market-analysis", "buyer-prospect",
            "deep-prospect", "lead-manage", "setup-sequences", "niche-sequential",
            "playbook",
        ],
        help=(
            "daily/historical = scrape notices; pdf-import/photo-import = import from files; "
            "dropbox-watch = poll Dropbox; csv-import = re-enrich CSV; "
            "phone-validate = Trestle scoring; manage-sold/manage-presets = DataSift ops; "
            "comp = comparable sales ARV; rehab = rehab cost estimate; "
            "analyze-deal = full deal analysis; market-analysis = zip code scoring; "
            "buyer-prospect = cash buyer lists; deep-prospect = 4-level research; "
            "lead-manage = 4 Pillars qualification; setup-sequences = CRM automation; "
            "niche-sequential = marketing cycle; playbook = SOP generator"
        ),
    )
    parser.add_argument(
        "--counties",
        type=str,
        default=None,
        help='Comma-separated counties to scrape (e.g. "Knox,Blount" or "all")',
    )
    parser.add_argument(
        "--types",
        type=str,
        default=None,
        help='Comma-separated notice types (e.g. "foreclosure,probate" or "all")',
    )
    parser.add_argument(
        "--split",
        action="store_true",
        help="Output separate CSV files per notice type",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Override date cutoff (YYYY-MM-DD). Overrides daily/historical mode logic.",
    )
    parser.add_argument(
        "--max-notices",
        type=int,
        default=0,
        help="Stop after scraping this many notices (0 = no limit)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )

    # PDF import arguments
    parser.add_argument(
        "--pdf-path",
        type=str,
        default=None,
        help="Path to scanned tax sale PDF (required for pdf-import mode)",
    )
    parser.add_argument(
        "--pdf-county",
        type=str,
        default=None,
        help='County name for PDF import, e.g. "Knox" (required for pdf-import mode)',
    )
    parser.add_argument(
        "--pdf-date",
        type=str,
        default=None,
        help="Date for PDF records (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--regex-only",
        action="store_true",
        help="Skip LLM parsing and use regex only (pdf-import mode)",
    )
    # Photo import arguments
    parser.add_argument(
        "--folder",
        type=str,
        default=None,
        help="Path to folder of phone photos (required for photo-import mode)",
    )
    parser.add_argument(
        "--photo-county",
        type=str,
        default=None,
        dest="photo_county",
        help='County name for photo import, e.g. "Knox" (required for photo-import mode)',
    )
    parser.add_argument(
        "--photo-type",
        type=str,
        default=None,
        dest="photo_type",
        help='Notice type for photo import, e.g. "eviction" (required for photo-import mode)',
    )
    parser.add_argument(
        "--photo-date",
        type=str,
        default=None,
        help="Date for photo records (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--no-perspective-correct",
        action="store_true",
        dest="no_perspective_correct",
        help="Skip perspective correction in photo preprocessing (photo-import mode)",
    )
    # Dropbox watcher arguments
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=None,
        dest="poll_interval",
        help="Seconds between Dropbox polls (default: 900 = 15 min)",
    )
    parser.add_argument(
        "--max-polls",
        type=int,
        default=None,
        dest="max_polls",
        help="Maximum number of poll cycles (default: infinite)",
    )
    parser.add_argument(
        "--no-delete",
        action="store_true",
        dest="no_delete",
        help="Don't delete photos from Dropbox after processing",
    )
    # CSV import arguments
    parser.add_argument(
        "--csv-path",
        type=str,
        default=None,
        help="Path to existing CSV file to re-enrich (required for csv-import mode)",
    )
    parser.add_argument(
        "--csv-county",
        type=str,
        default=None,
        help='County name for CSV import, e.g. "Knox" (sets county for records missing it)',
    )

    parser.add_argument(
        "--skip-smarty",
        action="store_true",
        help="Skip Smarty address standardization",
    )
    parser.add_argument(
        "--skip-zillow",
        action="store_true",
        help="Skip Zillow property enrichment",
    )
    parser.add_argument(
        "--skip-tax",
        action="store_true",
        help="Skip tax delinquency enrichment",
    )
    parser.add_argument(
        "--skip-obituary",
        action="store_true",
        help="Skip obituary search for deceased owner detection",
    )
    parser.add_argument(
        "--skip-ancestry",
        action="store_true",
        help="Skip Ancestry.com lookup (SSDI + obituary collection)",
    )
    parser.add_argument(
        "--skip-geocode",
        action="store_true",
        help="Skip reverse geocode retry for failed Smarty lookups",
    )
    parser.add_argument(
        "--skip-dm-address",
        action="store_true",
        help="Skip decision-maker mailing address lookup",
    )
    parser.add_argument(
        "--skip-heir-verification",
        action="store_true",
        help="Skip heir alive/dead verification loop (still runs obituary search)",
    )
    parser.add_argument(
        "--max-heir-depth",
        type=int,
        default=2,
        help="Max recursion depth for heir verification (default: 2)",
    )
    parser.add_argument(
        "--tracerfy-tier1",
        action="store_true",
        help="Use Tracerfy as primary DM address lookup ($0.02/record)",
    )
    parser.add_argument(
        "--skip-tracerfy",
        action="store_true",
        help="Skip Tracerfy batch skip trace (phones + emails) before DataSift upload",
    )
    parser.add_argument(
        "--llm-backend",
        choices=["anthropic", "ollama", "openrouter"],
        default=os.getenv("LLM_BACKEND", "anthropic"),
        help="LLM backend: 'anthropic' (Claude Haiku, paid) or 'ollama' (local, free)",
    )
    parser.add_argument(
        "--research-entities",
        action="store_true",
        help="Research entity-owned properties to find the person behind LLCs/Corps (web search + LLM)",
    )
    # Buy box / filter toggles — control which property types pass through
    parser.add_argument(
        "--include-vacant",
        action="store_true",
        help="Keep vacant land parcels (default: filtered out). Use if your buy box includes land deals.",
    )
    parser.add_argument(
        "--include-commercial",
        action="store_true",
        help="Keep commercial properties (default: filtered out). Use if your buy box includes commercial.",
    )
    parser.add_argument(
        "--include-entities",
        action="store_true",
        help="Keep entity-owned records (LLC, Corp, etc.) without filtering. Default: removed unless --research-entities finds a person.",
    )
    parser.add_argument(
        "--upload-datasift",
        action="store_true",
        help="Upload results to DataSift.ai via Playwright (requires DATASIFT_EMAIL/PASSWORD)",
    )
    parser.add_argument(
        "--no-enrich",
        action="store_true",
        help="Skip DataSift property enrichment after upload",
    )
    parser.add_argument(
        "--no-skip-trace",
        action="store_true",
        help="Skip DataSift skip trace after upload",
    )
    parser.add_argument(
        "--notify-slack",
        action="store_true",
        help="Send run summary to Slack/Discord webhook (requires SLACK_WEBHOOK_URL)",
    )
    parser.add_argument(
        "--audit-records",
        action="store_true",
        help="Audit DataSift for incomplete records (future: daily check via Playwright)",
    )

    # Phone validation arguments
    parser.add_argument(
        "--list-name",
        type=str,
        default=None,
        help="DataSift list name to export phones from (phone-validate mode)",
    )
    parser.add_argument(
        "--preset-folder",
        type=str,
        default=None,
        help="DataSift preset folder to export phones from (phone-validate mode)",
    )
    parser.add_argument(
        "--all-records",
        action="store_true",
        help="Export all DataSift records for phone validation (phone-validate mode)",
    )
    parser.add_argument(
        "--estimate",
        action="store_true",
        help="Show phone validation cost estimate only, no API calls (phone-validate mode)",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip uploading phone tags back to DataSift (phone-validate mode)",
    )
    parser.add_argument(
        "--custom-tiers",
        type=str,
        default=None,
        help='JSON custom tier boundaries, e.g. \'{"Hot": [80,100], "Cold": [0,79]}\' (phone-validate mode)',
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Concurrent Trestle API requests per batch (phone-validate mode, default: 10)",
    )
    parser.add_argument(
        "--add-litigator",
        action="store_true",
        help="Include litigator risk check in phone validation (phone-validate mode)",
    )

    # Manage sold arguments
    parser.add_argument(
        "--months-back",
        type=int,
        default=1,
        help="Months of sales to pull from SiftMap (manage-sold mode, default: 1)",
    )
    parser.add_argument(
        "--min-sale-price",
        type=int,
        default=1000,
        help="Min sale price to exclude deed transfers (manage-sold mode, default: 1000)",
    )
    parser.add_argument(
        "--sold-tag-date",
        type=str,
        default=None,
        help="Tag date in YYYY-MM format (manage-sold mode, default: current month)",
    )

    # Manage presets arguments
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Discover and list all preset folders, presets, and sequences (manage-presets mode)",
    )
    parser.add_argument(
        "--add-sold-exclusion",
        action="store_true",
        help="Update existing presets to exclude Sold status/tag (manage-presets mode)",
    )
    parser.add_argument(
        "--create-sold-sequence",
        action="store_true",
        help="Create Sold Property Cleanup sequence (manage-presets mode)",
    )
    parser.add_argument(
        "--preset-folders",
        type=str,
        default=None,
        help='Comma-separated preset folder names to target (manage-presets mode, default: all)',
    )

    # ── New analysis & workflow mode arguments ────────────────────────
    # Comp analysis
    parser.add_argument("--address", type=str, default=None,
                        help="Property address (comp/rehab/analyze-deal modes)")
    parser.add_argument("--city", type=str, default=None,
                        help="Property city (comp/rehab/analyze-deal modes)")
    parser.add_argument("--zip-code", type=str, default=None,
                        help="Property ZIP code (comp/rehab/analyze-deal modes)")
    parser.add_argument("--radius", type=float, default=0.5,
                        help="Comp search radius in miles (comp mode, default: 0.5)")
    parser.add_argument("--months", type=int, default=6,
                        help="Comp lookback months (comp mode, default: 6)")

    # Rehab estimation
    parser.add_argument("--tier", type=int, default=2, choices=[1, 2, 3, 4],
                        help="Finish tier 1-4 (rehab mode, default: 2)")
    parser.add_argument("--scope", type=str, default="full", choices=["full", "wholetail"],
                        help="Rehab scope (rehab mode, default: full)")
    parser.add_argument("--region", type=str, default="knoxville",
                        help="Regional pricing (rehab mode, default: knoxville)")
    parser.add_argument("--sqft", type=int, default=0,
                        help="Property sqft override (rehab mode)")
    parser.add_argument("--bedrooms", type=int, default=0,
                        help="Bedrooms override (rehab mode)")
    parser.add_argument("--bathrooms", type=float, default=0,
                        help="Bathrooms override (rehab mode)")

    # Deal analysis
    parser.add_argument("--purchase-price", type=float, default=0,
                        help="Purchase price (analyze-deal mode, default: auto-calculate MAO)")
    parser.add_argument("--rehab-tier", type=int, default=2, choices=[1, 2, 3, 4],
                        help="Rehab tier for deal analysis (default: 2)")
    parser.add_argument("--exit-strategy", type=str, default="flip",
                        choices=["flip", "wholesale", "hold"],
                        help="Exit strategy (analyze-deal mode, default: flip)")

    # Market analysis
    parser.add_argument("--zip-codes", type=str, default=None,
                        help="Comma-separated ZIP codes to analyze (market-analysis mode)")
    parser.add_argument("--monthly-budget", type=float, default=5000,
                        help="Monthly marketing budget for allocation (market-analysis mode)")

    # Buyer prospecting
    parser.add_argument("--min-transactions", type=int, default=2,
                        help="Min transactions to qualify as investor (buyer-prospect mode)")

    # Deep prospecting
    parser.add_argument("--depth", type=int, default=3, choices=[1, 2, 3, 4],
                        help="Research depth level 1-4 (deep-prospect mode, default: 3)")

    # Lead management
    parser.add_argument("--lead-action", type=str, default="qualify",
                        choices=["qualify", "report"],
                        help="Lead management action (lead-manage mode)")

    # Sequence setup
    parser.add_argument("--seq-folder", type=str, default="all",
                        choices=["lead-management", "acquisitions", "transactions",
                                 "deep-prospecting", "default", "all"],
                        help="Sequence folder to create (setup-sequences mode)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without creating (setup-sequences/niche-sequential)")

    # Niche sequential
    parser.add_argument("--channel", type=str, default="sms",
                        choices=["sms", "call", "mail", "dp"],
                        help="Marketing channel (niche-sequential mode)")
    parser.add_argument("--day", type=int, default=1, choices=[1, 2, 3],
                        help="Cycle day 1-3 (niche-sequential mode)")
    parser.add_argument("--ns-action", type=str, default="execute",
                        choices=["execute", "setup-presets", "status"],
                        help="Niche sequential action (niche-sequential mode)")

    # Playbook
    parser.add_argument("--blueprint", type=str, default="wholesale",
                        choices=["wholesale", "flip", "hold", "hybrid"],
                        help="Investment blueprint (playbook mode)")
    parser.add_argument("--market", type=str, default="knoxville",
                        help="Target market (playbook mode)")
    parser.add_argument("--team-size", type=int, default=1,
                        help="Team size 1/2/5 (playbook mode)")

    args = parser.parse_args()

    # Apply LLM backend override from CLI flag
    if hasattr(args, "llm_backend") and args.llm_backend:
        import config as cfg
        cfg.LLM_BACKEND = args.llm_backend
        if args.llm_backend == "ollama":
            logging.info("LLM backend: Ollama (%s)", cfg.OLLAMA_MODEL)
        elif args.llm_backend == "openrouter":
            logging.info("LLM backend: OpenRouter (%s)", cfg.OPENROUTER_MODEL)

    setup_logging(args.verbose)

    # ── Preflight health checks ──────────────────────────────────────
    preflight_failures = _preflight_check(args.mode)
    if preflight_failures:
        for f in preflight_failures:
            logging.error("Preflight FAILED: %s", f)
        # Send Slack alert so unattended runs are visible
        try:
            from slack_notifier import notify_preflight_failure
            notify_preflight_failure(preflight_failures)
        except Exception:
            pass  # Don't fail on notification failure
        sys.exit(1)
    logging.info("Preflight checks passed")

    # ── New analysis & workflow modes ─────────────────────────────────

    if args.mode == "comp":
        if not args.address:
            print("ERROR: --address is required for comp mode")
            return
        from comp_analyzer import run_comp_analysis
        result = run_comp_analysis(
            address=args.address, city=args.city or "", zip_code=args.zip_code or "",
            radius=args.radius, months=args.months,
        )
        if "error" in result:
            logger.error("Comp analysis failed: %s", result["error"])
        else:
            print(f"Comp report: {result['report_path']}")
            arv = result["arv"]
            print(f"ARV: ${arv.arv_low:,.0f} (low) / ${arv.arv_mid:,.0f} (mid) / ${arv.arv_high:,.0f} (high)")
            print(f"Confidence: {arv.confidence} — {arv.confidence_reason}")
        return

    if args.mode == "rehab":
        if not args.address:
            print("ERROR: --address is required for rehab mode")
            return
        from rehab_estimator import run_rehab_estimate
        result = run_rehab_estimate(
            address=args.address, sqft=args.sqft, bedrooms=args.bedrooms or 3,
            bathrooms=args.bathrooms or 2.0, tier=args.tier, scope=args.scope,
            region=args.region,
        )
        full = result["full_estimate"]
        wt = result["wholetail_estimate"]
        print(f"Rehab report: {result['report_path']}")
        print(f"Full rehab: ${full.grand_total:,.0f} ({full.total_weeks:.0f} weeks)")
        print(f"Wholetail:  ${wt.grand_total:,.0f} ({wt.total_weeks:.0f} weeks)")
        return

    if args.mode == "analyze-deal":
        if not args.address:
            print("ERROR: --address is required for analyze-deal mode")
            return
        from deal_analyzer import run_deal_analysis
        result = run_deal_analysis(
            address=args.address, city=args.city or "", zip_code=args.zip_code or "",
            purchase_price=args.purchase_price, rehab_tier=args.rehab_tier,
            exit_strategy=args.exit_strategy, region=args.region,
            radius=args.radius, months=args.months,
        )
        if "error" in result:
            logger.error("Deal analysis failed: %s", result["error"])
        else:
            pkg = result["package"]
            print(f"Deal report: {result['report_path']}")
            print(f"Recommendation: {pkg.recommendation}")
            print(f"ARV: ${pkg.arv.arv_mid:,.0f} | Rehab: ${pkg.rehab_full.grand_total:,.0f}")
            print(f"Flip MAO: ${pkg.mao.flip_mao:,.0f} | Profit: ${pkg.flip.net_profit:,.0f} ({pkg.flip.roi_pct:.0f}% ROI)")
        return

    if args.mode == "market-analysis":
        from market_analyzer import run_market_analysis
        counties = args.counties.split(",") if args.counties else None
        zip_codes = args.zip_codes.split(",") if args.zip_codes else None
        result = run_market_analysis(
            counties=counties, zip_codes=zip_codes,
            monthly_budget=args.monthly_budget,
        )
        if "error" in result:
            logger.error("Market analysis failed: %s", result["error"])
        else:
            report = result["report"]
            print(f"Market report: {result['report_path']}")
            print(f"Analyzed {report.total_zips} zips, {report.total_notices} total notices")
            if report.top_zips:
                top = report.top_zips[0]
                print(f"Top zip: {top.zip_code} (score {top.score:.1f}, grade {top.grade})")
        return

    if args.mode == "buyer-prospect":
        from buyer_prospector import run_buyer_prospecting
        counties = args.counties.split(",") if args.counties else None
        result = run_buyer_prospecting(
            counties=counties,
            months_back=args.months_back,
            min_transactions=args.min_transactions,
        )
        if "error" in result:
            logger.error("Buyer prospecting failed: %s", result["error"])
        else:
            report = result["report"]
            print(f"Buyer report: {result['report_path']}")
            print(f"Found {report.total_investors} investors")
            print(f"CSV: {result.get('csv_path', 'N/A')}")
        return

    if args.mode == "deep-prospect":
        csv_path = args.csv_path if hasattr(args, "csv_path") and args.csv_path else ""
        if not csv_path:
            csvs = sorted(config.OUTPUT_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
            csv_path = str(csvs[0]) if csvs else ""
        if not csv_path:
            print("ERROR: --csv-path required or place CSVs in output/")
            return
        import asyncio
        from deep_prospector import run_deep_prospecting
        result = asyncio.run(run_deep_prospecting(
            csv_path=csv_path, depth=args.depth,
            max_records=args.max_notices if hasattr(args, "max_notices") else 0,
        ))
        if "error" in result:
            logger.error("Deep prospecting failed: %s", result["error"])
        else:
            stats = result["stats"]
            print(f"Report: {result['report_path']}")
            print(f"Processed {stats['total']} records at depth {args.depth}")
            print(f"Phones: {stats['phones_found']} | Deceased: {stats['deceased_confirmed']} | DMs: {stats['dms_identified']}")
        return

    if args.mode == "lead-manage":
        from lead_manager import run_lead_management
        csv_path = args.csv_path if hasattr(args, "csv_path") and args.csv_path else ""
        result = run_lead_management(
            action=args.lead_action, csv_path=csv_path,
        )
        if "error" in result:
            logger.error("Lead management failed: %s", result["error"])
        else:
            print(f"STABM report: {result['report_path']}")
            print(f"Total: {result['total']} | Hot: {result['hot']} | Warm: {result['warm']} | Cold: {result['cold']}")
        return

    if args.mode == "setup-sequences":
        from sequence_templates import get_templates, list_templates, preview_sequence
        templates = get_templates(args.seq_folder)
        if args.dry_run:
            print(f"DRY RUN — Would create {len(templates)} sequences in DataSift:")
            for t in templates:
                preview = preview_sequence(t)
                print(f"  [{preview['folder']}] {preview['name']}")
                print(f"    Trigger: {preview['trigger']}")
                print(f"    Actions: {len(preview['actions'])}")
        else:
            print(f"Sequence creation requires Playwright — {len(templates)} templates ready")
            print("Templates defined. DataSift Playwright creation coming in next build.")
            print("\nTemplate list:")
            print(list_templates())
        return

    if args.mode == "niche-sequential":
        from niche_sequential import run_niche_sequential
        result = run_niche_sequential(
            list_name=args.list_name or "",
            channel=args.channel, day=args.day,
            csv_path=args.csv_path if hasattr(args, "csv_path") and args.csv_path else "",
            action=args.ns_action,
        )
        if "error" in result:
            logger.error("Niche sequential failed: %s", result["error"])
        elif "output" in result:
            print(f"Exported: {result['output']}")
            print(f"Channel: {result['channel']}, Day {result['day']}, {result['records']} records")
        elif "presets" in result:
            for p in result["presets"]:
                print(f"  {p['name']}: {p['description']}")
        return

    if args.mode == "playbook":
        from playbook_generator import run_playbook_generator
        result = run_playbook_generator(
            blueprint=args.blueprint, market=args.market,
            team_size=args.team_size,
        )
        print(f"Playbook: {result['playbook_path']}")
        print(f"Blueprint: {result['blueprint'].title()} | Market: {result['market'].title()} | Team: {result['team_size']}")
        return

    # Phone validation mode — separate pipeline
    if args.mode == "phone-validate":
        _run_phone_validate(args)
        return

    # Manage presets mode — filter preset + sequence management
    if args.mode == "manage-presets":
        _run_manage_presets(args)
        return

    # Manage sold properties mode — SiftMap workflow
    if args.mode == "manage-sold":
        _run_manage_sold(args)
        return

    # PDF import mode — separate pipeline
    if args.mode == "pdf-import":
        _run_pdf_import(args)
        return

    # Photo import mode — separate pipeline
    if args.mode == "photo-import":
        _run_photo_import(args)
        return

    # Dropbox watcher mode — polls for new photos
    if args.mode == "dropbox-watch":
        from dropbox_watcher import run_watcher
        run_watcher(
            poll_interval=args.poll_interval,
            delete_after=not getattr(args, "no_delete", False),
            max_polls=args.max_polls,
        )
        return

    # CSV re-import mode — separate pipeline
    if args.mode == "csv-import":
        _run_csv_import(args)
        return

    # Filter saved searches
    counties = None
    if args.counties and args.counties.lower() != "all":
        counties = [c.strip() for c in args.counties.split(",")]

    types = None
    if args.types and args.types.lower() != "all":
        types = [t.strip() for t in args.types.split(",")]

    searches = _filter_searches(counties, types)
    if not searches:
        logging.error("No saved searches match the given --counties / --types filters")
        sys.exit(1)

    logging.info(
        "Running %d saved searches: %s",
        len(searches),
        ", ".join(s.saved_search_name for s in searches),
    )

    try:
        _run_scrape_pipeline(args, searches)
    except Exception as e:
        logging.exception("Pipeline failed with unhandled error")
        try:
            from slack_notifier import notify_error
            notify_error("Pipeline (top-level)", e, context=f"mode={args.mode}")
        except Exception:
            pass
        sys.exit(1)


def _run_scrape_pipeline(args, searches) -> None:
    """Run the daily/historical scrape → enrich → export → upload pipeline."""
    # Scrape
    notices = asyncio.run(scrape_all(
        mode=args.mode, searches=searches,
        llm_api_key=config.ANTHROPIC_API_KEY or None,
        since_date_override=args.since,
        max_notices=args.max_notices,
    ))
    # Handle async probate lookup before pipeline (requires asyncio.run)
    probate_notices = [n for n in notices if n.notice_type == "probate" and n.decedent_name and not n.address]
    if probate_notices:
        try:
            from property_lookup import lookup_decedent_properties
            logging.info("Looking up property addresses for %d probate notices...", len(probate_notices))
            asyncio.run(lookup_decedent_properties(probate_notices))
        except ImportError:
            logging.warning("property_lookup module not found -- skipping property lookup")
        except Exception as e:
            logging.warning("Property lookup failed: %s -- continuing without lookups", e)

    # Run unified enrichment pipeline
    from enrichment_pipeline import PipelineOptions, run_enrichment_pipeline

    opts = PipelineOptions(
        skip_parcel_lookup=True,  # web scrape notices don't have parcel IDs
        skip_vacant_filter=getattr(args, "include_vacant", False),
        skip_commercial_filter=getattr(args, "include_commercial", False),
        skip_entity_filter=getattr(args, "include_entities", False),
        skip_smarty=getattr(args, "skip_smarty", False),
        skip_zillow=getattr(args, "skip_zillow", False),
        skip_tax=getattr(args, "skip_tax", False),
        skip_geocode=getattr(args, "skip_geocode", False),
        skip_obituary=args.skip_obituary,
        skip_ancestry=getattr(args, "skip_ancestry", False),
        skip_entity_research=not getattr(args, "research_entities", False),
        skip_heir_verification=args.skip_heir_verification,
        max_heir_depth=args.max_heir_depth,
        skip_dm_address=args.skip_dm_address,
        tracerfy_tier1=getattr(args, "tracerfy_tier1", False),
        source_label=f"CLI {args.mode}",
    )
    notices = run_enrichment_pipeline(notices, opts)

    if not notices:
        logging.warning("No notices found")
        sys.exit(0)

    # Tracerfy batch skip trace (phones + emails for all records)
    if not getattr(args, "skip_tracerfy", False):
        import config as cfg
        if cfg.TRACERFY_API_KEY:
            from tracerfy_skip_tracer import batch_skip_trace
            tracerfy_stats = batch_skip_trace(notices)
            logging.info(
                "Tracerfy: %d/%d matched, %d phones, %d emails, $%.2f",
                tracerfy_stats["matched"], tracerfy_stats["submitted"],
                tracerfy_stats["phones_found"], tracerfy_stats["emails_found"],
                tracerfy_stats["cost"],
            )

    # Write output
    if args.split:
        paths = write_csv_by_type(notices)
        for p in paths:
            logging.info("Output: %s", p)
    else:
        path = write_csv(notices)
        logging.info("Output: %s", path)

    # DataSift upload
    upload_result = None
    if getattr(args, "upload_datasift", False):
        from datasift_formatter import write_datasift_csv, write_datasift_split_csvs
        from datasift_uploader import upload_to_datasift, upload_datasift_split

        do_enrich = not getattr(args, "no_enrich", False)
        do_skip_trace = not getattr(args, "no_skip_trace", False)

        # Use split flow (separate DM + Heir Map Message Board entries)
        csv_infos = write_datasift_split_csvs(notices)
        for info in csv_infos:
            logging.info("DataSift CSV (%s): %s", info["label"], info["path"])

        if len(csv_infos) > 1:
            upload_result = asyncio.run(
                upload_datasift_split(
                    csv_infos,
                    enrich=do_enrich,
                    skip_trace=do_skip_trace,
                )
            )
        else:
            # No deceased-with-heirs — single CSV upload
            upload_result = asyncio.run(
                upload_to_datasift(
                    csv_infos[0]["path"],
                    enrich=do_enrich,
                    skip_trace=do_skip_trace,
                )
            )

        if upload_result.get("success"):
            logging.info("DataSift upload: %s", upload_result.get("message", "OK"))
            if upload_result.get("enrich_result"):
                logging.info("  Enrich: %s", upload_result["enrich_result"].get("message", ""))
            if upload_result.get("skip_trace_result"):
                logging.info("  Skip trace: %s", upload_result["skip_trace_result"].get("message", ""))
        else:
            logging.error("DataSift upload failed: %s", upload_result.get("message"))

    # Slack/Discord notification
    if getattr(args, "notify_slack", False):
        from slack_notifier import send_slack_notification

        send_slack_notification(notices, upload_result=upload_result)

    # Audit DataSift for incomplete records (future daily check)
    if getattr(args, "audit_records", False):
        logging.info("--audit-records: Not yet implemented. "
                      "Will check DataSift Incomplete tab via Playwright in a future build.")

    logging.info("Done — %d notices exported", len(notices))


# ── Entry point ───────────────────────────────────────────────────────


if __name__ == "__main__":
    if os.environ.get("APIFY_IS_AT_HOME") or os.environ.get("APIFY_TOKEN"):
        # Running inside Apify platform or with apify run
        asyncio.run(actor_main())
    else:
        # Standalone CLI
        cli_main()
