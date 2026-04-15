"""Generate a PDF deep-prospecting report for a single enriched record.

Produces a professional multi-page PDF with property summary, valuation,
deceased owner detection, signing chain with skip-trace contacts, and
tax delinquency data.
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
)

import case_summary
import config
from notice_parser import NoticeData

logger = logging.getLogger(__name__)

# ── Brand Colors ─────────────────────────────────────────────────────

BRAND_DARK = colors.HexColor("#1a1a2e")
BRAND_PRIMARY = colors.HexColor("#16213e")
BRAND_ACCENT = colors.HexColor("#0f3460")
BRAND_HIGHLIGHT = colors.HexColor("#e94560")
BRAND_LIGHT_BG = colors.HexColor("#f5f6fa")
BRAND_BORDER = colors.HexColor("#dcdde1")
BRAND_SUCCESS = colors.HexColor("#2ecc71")
BRAND_WARNING = colors.HexColor("#f39c12")
BRAND_MUTED = colors.HexColor("#7f8c8d")
ROW_ALT = colors.HexColor("#f8f9fa")
WHITE = colors.white

# ── Styles ───────────────────────────────────────────────────────────

_styles = getSampleStyleSheet()

TITLE_STYLE = ParagraphStyle(
    "ReportTitle",
    parent=_styles["Title"],
    fontSize=20,
    fontName="Helvetica-Bold",
    textColor=BRAND_DARK,
    spaceAfter=2,
    leading=24,
)

SUBTITLE_STYLE = ParagraphStyle(
    "Subtitle",
    parent=_styles["Normal"],
    fontSize=10,
    textColor=BRAND_MUTED,
    spaceAfter=12,
)

SECTION_STYLE = ParagraphStyle(
    "SectionHeader",
    parent=_styles["Heading2"],
    fontSize=11,
    fontName="Helvetica-Bold",
    textColor=WHITE,
    spaceBefore=14,
    spaceAfter=0,
    leading=16,
    leftIndent=6,
)

BODY_STYLE = ParagraphStyle(
    "BodyText",
    parent=_styles["Normal"],
    fontSize=9,
    leading=13,
    fontName="Helvetica",
)

BODY_BOLD = ParagraphStyle(
    "BodyBold",
    parent=BODY_STYLE,
    fontName="Helvetica-Bold",
)

SMALL_STYLE = ParagraphStyle(
    "SmallText",
    parent=_styles["Normal"],
    fontSize=8,
    leading=10,
    textColor=BRAND_MUTED,
)

SIGNER_STYLE = ParagraphStyle(
    "SignerName",
    parent=_styles["Normal"],
    fontSize=10,
    leading=14,
    fontName="Helvetica-Bold",
    textColor=BRAND_DARK,
    spaceBefore=8,
    spaceAfter=2,
)

BADGE_ALIVE = ParagraphStyle(
    "BadgeAlive",
    parent=BODY_STYLE,
    textColor=BRAND_SUCCESS,
    fontName="Helvetica-Bold",
)

BADGE_DECEASED = ParagraphStyle(
    "BadgeDeceased",
    parent=BODY_STYLE,
    textColor=BRAND_HIGHLIGHT,
    fontName="Helvetica-Bold",
)

FOOTER_STYLE = ParagraphStyle(
    "Footer",
    parent=_styles["Normal"],
    fontSize=7,
    textColor=BRAND_MUTED,
    alignment=1,  # center
)


# ── Helpers ──────────────────────────────────────────────────────────

def _val(value: str, fallback: str = "—") -> str:
    return value.strip() if value and value.strip() else fallback


def _money(value: str) -> str:
    if not value:
        return "—"
    try:
        return f"${int(float(value)):,}"
    except (ValueError, TypeError):
        return value


def _section_header(title: str) -> Table:
    """Create a colored section header bar."""
    t = Table(
        [[Paragraph(title, SECTION_STYLE)]],
        colWidths=[6.5 * inch],
        rowHeights=[22],
    )
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND_PRIMARY),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("ROUNDEDCORNERS", [4, 4, 0, 0]),
    ]))
    return t


def _data_table(data: list[tuple[str, str]], col_widths=None) -> Table:
    """Build a bordered key-value table with alternating row shading."""
    if col_widths is None:
        col_widths = [1.8 * inch, 4.7 * inch]

    table_data = []
    for label, value in data:
        table_data.append([
            Paragraph(f"<b>{label}</b>", BODY_STYLE),
            Paragraph(str(value), BODY_STYLE),
        ])

    style_cmds = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (0, -1), 8),
        ("LEFTPADDING", (1, 0), (1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.5, BRAND_BORDER),
        ("LINEBELOW", (0, -1), (-1, -1), 0.5, BRAND_BORDER),
    ]

    # Alternating row shading
    for i in range(len(table_data)):
        if i % 2 == 0:
            style_cmds.append(("BACKGROUND", (0, i), (-1, i), ROW_ALT))

    t = Table(table_data, colWidths=col_widths)
    t.setStyle(TableStyle(style_cmds))
    return t


def _status_badge(status: str) -> str:
    """Return colored status text."""
    s = status.lower().replace("_", " ")
    if "living" in s or "alive" in s:
        return f'<font color="#2ecc71"><b>VERIFIED ALIVE</b></font>'
    elif "deceased" in s:
        return f'<font color="#e94560"><b>DECEASED</b></font>'
    elif "unverified" in s:
        return f'<font color="#f39c12"><b>UNVERIFIED</b></font>'
    return s.upper()


def _confidence_badge(level: str) -> str:
    """Return colored confidence text."""
    if level.lower() == "high":
        return f'<font color="#2ecc71"><b>HIGH</b></font>'
    elif level.lower() == "medium":
        return f'<font color="#f39c12"><b>MEDIUM</b></font>'
    elif level.lower() == "low":
        return f'<font color="#e94560"><b>LOW</b></font>'
    return level.upper()


def _address_slug(notice: NoticeData) -> str:
    addr = (notice.address or "unknown").lower()
    slug = re.sub(r"[^a-z0-9]+", "_", addr).strip("_")
    return slug[:50]


# ── Case Summary helpers (deceased-owner streamline section) ─────────

def _bullet_list(items: list[str]) -> list:
    """Return a list of Paragraph flowables rendering plain strings as bullets."""
    return [Paragraph(f"&#8226;&nbsp; {item}", BODY_STYLE) for item in items if item]


def _family_tree_table(grouped: dict[str, list[dict]]) -> Table | None:
    """Render grouped heirs as a two-column relationship → names table."""
    if not grouped:
        return None

    rows = []
    for group_label, heirs in grouped.items():
        lines = []
        for heir in heirs:
            name = (heir.get("name") or "").strip() or "—"
            status = (heir.get("status") or "").lower()
            if "deceased" in status:
                name_html = f'<font color="#e94560">{name}</font>'
            elif "living" in status:
                name_html = f'<font color="#2ecc71">{name}</font>'
            else:
                name_html = f'<font color="#f39c12">{name}</font>'
            badges = []
            phones = heir.get("phones") or []
            emails = heir.get("emails") or []
            if phones:
                badges.append(f"{len(phones)}ph")
            if emails:
                badges.append(f"{len(emails)}em")
            if heir.get("signing_authority"):
                badges.append("signer")
            badge_str = f' <font color="#7f8c8d">[{", ".join(badges)}]</font>' if badges else ""
            rel = (heir.get("relationship") or "").strip()
            rel_str = f' <font color="#7f8c8d">— {rel}</font>' if rel else ""
            lines.append(f"{name_html}{rel_str}{badge_str}")
        rows.append((group_label, "<br/>".join(lines) or "—"))
    return _data_table(rows, col_widths=[1.8 * inch, 4.7 * inch])


def _add_case_summary(story: list, notice: NoticeData) -> None:
    """Render the Case Summary block (deceased-owner records only)."""
    story.append(_section_header("Case Summary"))

    # Situation prose
    prose = case_summary.build_situation_prose(
        notice, api_key=getattr(config, "ANTHROPIC_API_KEY", "") or None,
    )
    if prose:
        story.append(Spacer(1, 4))
        story.append(Paragraph(f"<b>Situation:</b> {prose}", BODY_STYLE))
        story.append(Spacer(1, 6))

    # Key findings
    findings = case_summary.build_key_findings(notice)
    if findings:
        story.append(Paragraph("<b>Key Findings</b>", BODY_BOLD))
        for flow in _bullet_list(findings):
            story.append(flow)
        story.append(Spacer(1, 6))

    # Family tree (grouped heirs)
    grouped = case_summary.group_heirs(notice.heir_map_json)
    if grouped:
        story.append(Paragraph("<b>Family Tree</b>", BODY_BOLD))
        story.append(Spacer(1, 2))
        tree = _family_tree_table(grouped)
        if tree is not None:
            story.append(tree)
        story.append(Spacer(1, 6))

    # Recommended next steps
    steps = case_summary.build_next_steps(notice)
    if steps:
        story.append(Paragraph("<b>Recommended Next Steps</b>", BODY_BOLD))
        for flow in _bullet_list(steps):
            story.append(flow)
        story.append(Spacer(1, 6))


# ── Main Generator ───────────────────────────────────────────────────

def generate_record_pdf(
    notice: NoticeData,
    output_dir: Path | None = None,
    phone_tiers: dict | None = None,
) -> Path:
    """Generate a professional PDF deep-prospecting report.

    Args:
        notice: Fully enriched NoticeData object.
        output_dir: Directory for the PDF (default: output/reports/).
        phone_tiers: Optional dict mapping cleaned phone -> {score, tier, line_type}

    Returns:
        Path to the generated PDF file.
    """
    if output_dir is None:
        output_dir = Path("output/reports")
    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{_address_slug(notice)}_{date_str}.pdf"
    pdf_path = output_dir / filename

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
    )

    story = []

    # ── Header ──────────────────────────────────────────────────
    story.append(Paragraph("Deep Prospecting Report", TITLE_STYLE))
    addr_line = f"{_val(notice.address)}, {_val(notice.city)}, {_val(notice.state)} {_val(notice.zip)}"
    story.append(Paragraph(
        f"{addr_line}&nbsp;&nbsp;|&nbsp;&nbsp;{date_str}&nbsp;&nbsp;|&nbsp;&nbsp;"
        f"{_val(notice.notice_type).replace('_', ' ').title()} — {_val(notice.county)} County",
        SUBTITLE_STYLE,
    ))
    story.append(HRFlowable(
        width="100%", thickness=2, color=BRAND_HIGHLIGHT,
        spaceAfter=10, spaceBefore=0,
    ))

    # ── Case Summary (deceased-owner records only) ──────────────
    # Rendered first so an acquisitions manager can read the situation,
    # findings, family tree, and next steps before scrolling into detail tables.
    if notice.owner_deceased == "yes":
        _add_case_summary(story, notice)

    # ── Property Details ────────────────────────────────────────
    story.append(_section_header("Property Details"))
    story.append(_data_table([
        ("Address", addr_line),
        ("County", _val(notice.county)),
        ("Parcel ID", _val(notice.parcel_id)),
        ("Property Type", _val(notice.property_type)),
        ("Beds / Baths", f"{_val(notice.bedrooms)} bd / {_val(notice.bathrooms)} ba"),
        ("Living Sqft", f"{_val(notice.sqft)} sqft" if notice.sqft else "—"),
        ("Year Built", _val(notice.year_built)),
        ("Lot Size", f"{_val(notice.lot_size)} sqft" if notice.lot_size else "—"),
        ("USPS Deliverable", "Yes (DPV: Y)" if notice.dpv_match_code == "Y"
            else f"No (DPV: {_val(notice.dpv_match_code)})" if notice.dpv_match_code else "—"),
        ("Vacancy", "Vacant" if notice.vacant == "Y" else "Occupied" if notice.vacant else "—"),
        ("Coordinates", f"{notice.latitude}, {notice.longitude}" if notice.latitude else "—"),
    ]))
    story.append(Spacer(1, 6))

    # ── Notice & Valuation (side by side concept, but stacked for simplicity) ──
    story.append(_section_header("Notice Information"))
    notice_rows = [
        ("Notice Type", _val(notice.notice_type).replace("_", " ").title()),
        ("Date Added", _val(notice.date_added)),
        ("Owner on Title", _val(notice.owner_name)),
    ]
    if notice.auction_date:
        notice_rows.append(("Auction Date", _val(notice.auction_date)))
    if notice.source_url:
        notice_rows.append(("Source", _val(notice.source_url)))
    story.append(_data_table(notice_rows))
    story.append(Spacer(1, 6))

    story.append(_section_header("Valuation & Equity"))
    val_rows = [
        ("Estimated Value (Zestimate)", _money(notice.estimated_value)),
        ("Estimated Equity", _money(notice.estimated_equity)),
        ("Equity %", f"{_val(notice.equity_percent)}%" if notice.equity_percent else "—"),
        ("MLS Status", _val(notice.mls_status)),
    ]
    if notice.mls_last_sold_date:
        val_rows.append(("Last Sale", f"{_val(notice.mls_last_sold_date)} @ {_money(notice.mls_last_sold_price)}"))
    if notice.mls_listing_price:
        val_rows.append(("Listing Price", _money(notice.mls_listing_price)))
    story.append(_data_table(val_rows))
    story.append(Spacer(1, 6))

    # ── Tax Delinquency ─────────────────────────────────────────
    if notice.tax_delinquent_amount:
        story.append(_section_header("Tax Delinquency"))
        story.append(_data_table([
            ("Amount Due", _money(notice.tax_delinquent_amount)),
            ("Years Delinquent", _val(notice.tax_delinquent_years)),
            ("Tax Owner of Record", _val(notice.tax_owner_name)),
        ]))
        story.append(Spacer(1, 6))

    # ── Deceased Owner Detection ────────────────────────────────
    if notice.owner_deceased == "yes":
        story.append(_section_header("Deceased Owner Detection"))
        conf_text = _confidence_badge(notice.dm_confidence) if notice.dm_confidence else "—"
        if notice.dm_confidence_reason:
            conf_text += f" — {notice.dm_confidence_reason}"
        story.append(_data_table([
            ("Status", '<font color="#e94560"><b>DECEASED</b></font>'),
            ("Date of Death", _val(notice.date_of_death)),
            ("Decedent", _val(notice.decedent_name) if notice.decedent_name else _val(notice.owner_name)),
            ("Obituary Source", _val(notice.obituary_source_type).replace("_", " ").title()),
            ("Obituary URL", _val(notice.obituary_url)),
            ("DM Confidence", conf_text),
        ]))
        story.append(Spacer(1, 6))

        # ── Decision Maker ──────────────────────────────────────
        story.append(_section_header("Decision Maker (Primary Contact)"))
        dm_addr = ""
        if notice.decision_maker_street:
            dm_addr = (f"{notice.decision_maker_street}, "
                       f"{_val(notice.decision_maker_city)}, "
                       f"{_val(notice.decision_maker_state)} "
                       f"{_val(notice.decision_maker_zip)}")
        story.append(_data_table([
            ("Name", f"<b>{_val(notice.decision_maker_name)}</b>"),
            ("Relationship", _val(notice.decision_maker_relationship).title()),
            ("Status", _status_badge(_val(notice.decision_maker_status))),
            ("Source", _val(notice.decision_maker_source).replace("_", " ").title()),
            ("Mailing Address", dm_addr or "—"),
        ]))
        story.append(Spacer(1, 6))

        # ── Signing Chain ───────────────────────────────────────
        chain_count = _val(notice.signing_chain_count, "0")
        story.append(_section_header(f"Signing Chain ({chain_count} heirs must sign)"))
        _add_signing_chain(story, notice, phone_tiers)

    # ── Skip Trace Contacts ─────────────────────────────────────
    # Show all phones/emails even for non-deceased (living owner contacts)
    has_phones = any(getattr(notice, f, "") for f in
                     ["primary_phone", "mobile_1", "mobile_2", "mobile_3",
                      "mobile_4", "mobile_5", "landline_1", "landline_2", "landline_3"])
    has_emails = any(getattr(notice, f, "") for f in
                     ["email_1", "email_2", "email_3", "email_4", "email_5"])

    if (has_phones or has_emails) and notice.owner_deceased != "yes":
        story.append(_section_header("Skip Trace Contacts"))
        contact_rows = []

        # Phones
        from phone_validator import clean_phone
        for label, field in [("Mobile 1", "mobile_1"), ("Mobile 2", "mobile_2"),
                             ("Mobile 3", "mobile_3"), ("Mobile 4", "mobile_4"),
                             ("Mobile 5", "mobile_5"), ("Landline 1", "landline_1"),
                             ("Landline 2", "landline_2"), ("Landline 3", "landline_3")]:
            val = getattr(notice, field, "")
            if val:
                tier_str = ""
                if phone_tiers:
                    cleaned = clean_phone(val)
                    info = phone_tiers.get(cleaned, {})
                    if info.get("tier"):
                        tier_str = f'  <font color="#7f8c8d">[{info["tier"]}, score={info["score"]}, {info.get("line_type", "")}]</font>'
                contact_rows.append((label, f"{val}{tier_str}"))

        # Emails
        for i in range(1, 6):
            val = getattr(notice, f"email_{i}", "")
            if val:
                contact_rows.append((f"Email {i}", val))

        if contact_rows:
            story.append(_data_table(contact_rows))
        story.append(Spacer(1, 6))

    # ── Footer ──────────────────────────────────────────────────
    story.append(Spacer(1, 12))
    story.append(HRFlowable(
        width="100%", thickness=0.5, color=BRAND_BORDER,
        spaceAfter=4, spaceBefore=0,
    ))
    footer_parts = [f"SiftStack Deep Prospecting Report"]
    if notice.run_id:
        footer_parts.append(f"Run: {notice.run_id}")
    footer_parts.append(date_str)
    story.append(Paragraph(" &nbsp;|&nbsp; ".join(footer_parts), FOOTER_STYLE))

    doc.build(story)
    logger.info("PDF report generated: %s", pdf_path)
    return pdf_path


def _add_signing_chain(
    story: list,
    notice: NoticeData,
    phone_tiers: dict | None,
) -> None:
    """Add signing chain heirs with contact info to the PDF story."""
    if not notice.heir_map_json:
        story.append(Paragraph("(no heir map generated)", BODY_STYLE))
        return

    try:
        heirs = json.loads(notice.heir_map_json)
    except (json.JSONDecodeError, TypeError):
        story.append(Paragraph("(heir map parse error)", BODY_STYLE))
        return

    signers = [h for h in heirs
               if h.get("signing_authority") and h.get("status") != "deceased"]
    non_signers = [h for h in heirs
                   if not h.get("signing_authority") or h.get("status") == "deceased"]

    from phone_validator import clean_phone

    for i, h in enumerate(signers, 1):
        name = h.get("name", "?")
        rel = h.get("relationship", "?").title()
        status = _status_badge(h.get("status", "?"))

        story.append(Paragraph(
            f"#{i}&nbsp;&nbsp;{name}&nbsp;&nbsp;({rel})&nbsp;&nbsp;{status}",
            SIGNER_STYLE,
        ))

        rows = []

        # Address
        if h.get("street"):
            addr = (f"{h['street']}, {h.get('city', '')}, "
                    f"{h.get('state', '')} {h.get('zip', '')}")
            rows.append(("Mailing Address", addr))

        # Phones
        phones = h.get("phones", [])
        if i == 1 and not phones:
            for field in ["primary_phone", "mobile_1", "mobile_2", "mobile_3",
                          "mobile_4", "mobile_5", "landline_1", "landline_2",
                          "landline_3"]:
                val = getattr(notice, field, "")
                if val:
                    phones.append(val)

        for j, ph in enumerate(phones, 1):
            tier_str = ""
            if phone_tiers:
                cleaned = clean_phone(ph)
                info = phone_tiers.get(cleaned, {})
                if info.get("tier"):
                    tier_str = f'  <font color="#7f8c8d">[{info["tier"]}, score={info["score"]}, {info.get("line_type", "")}]</font>'
            rows.append((f"Phone {j}", f"{ph}{tier_str}"))

        # Emails
        emails = h.get("emails", [])
        for j, em in enumerate(emails, 1):
            rows.append((f"Email {j}", em))

        if not phones and not emails:
            rows.append(("Contact", '<font color="#f39c12"><i>No phone or email found — needs skip trace</i></font>'))

        story.append(_data_table(rows, col_widths=[1.4 * inch, 5.1 * inch]))
        story.append(Spacer(1, 2))

    # Non-signers
    if non_signers:
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            f"<b>Other Family ({len(non_signers)} — no signing authority)</b>",
            BODY_STYLE,
        ))
        lines = []
        for h in non_signers[:8]:
            status = _status_badge(h.get("status", "?"))
            lines.append(
                f"{h.get('name', '?')} ({h.get('relationship', '?').title()}) — {status}"
            )
        if len(non_signers) > 8:
            lines.append(f"... and {len(non_signers) - 8} more")
        story.append(Paragraph("<br/>".join(lines), SMALL_STYLE))
