"""Case summary generation for deceased-owner deep-prospecting records.

Produces the four building blocks that feed the Case Summary PDF section:
  - situation prose (LLM if available, template fallback)
  - key findings bullets (templated from NoticeData fields)
  - grouped family tree from heir_map_json
  - recommended next steps bullets (templated from record state)

Designed to be pure data-in/strings-out so report_generator.py can flow the
results into its existing Paragraph/Table styles without coupling.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import config
import llm_client
from notice_parser import NoticeData

logger = logging.getLogger(__name__)


# ── Relationship grouping for the family tree ────────────────────────

# Order matters — this is how groups render in the PDF, top to bottom.
RELATIONSHIP_GROUPS: list[tuple[str, list[str]]] = [
    ("Spouse", ["spouse", "wife", "husband", "partner"]),
    ("Children", ["son", "daughter", "child", "stepson", "stepdaughter"]),
    ("Grandchildren", ["grandson", "granddaughter", "grandchild"]),
    ("Parents", ["father", "mother", "parent"]),
    ("Siblings", ["brother", "sister", "sibling"]),
    ("Nieces / Nephews", ["niece", "nephew"]),
    ("In-Laws", ["son-in-law", "daughter-in-law", "brother-in-law", "sister-in-law",
                 "father-in-law", "mother-in-law"]),
    ("Executor / PR", ["executor", "personal representative", "administrator", "pr"]),
]


def _classify_relationship(rel: str) -> str:
    """Return the display group for a relationship string, or 'Other Heirs'."""
    r = (rel or "").lower().strip()
    if not r:
        return "Other Heirs"
    for label, keys in RELATIONSHIP_GROUPS:
        if any(k in r for k in keys):
            return label
    return "Other Heirs"


def group_heirs(heir_map_json: str) -> dict[str, list[dict[str, Any]]]:
    """Parse heir_map_json and group heirs by relationship category.

    Returns ordered dict-like structure: {group_label: [heir_obj, ...]}.
    Preserves RELATIONSHIP_GROUPS order; appends 'Other Heirs' last if present.
    Returns empty dict on malformed input.
    """
    if not heir_map_json:
        return {}
    try:
        heirs = json.loads(heir_map_json)
    except (ValueError, TypeError):
        logger.debug("heir_map_json malformed; skipping family tree")
        return {}
    if not isinstance(heirs, list):
        return {}

    grouped: dict[str, list[dict[str, Any]]] = {}
    for heir in heirs:
        if not isinstance(heir, dict):
            continue
        label = _classify_relationship(heir.get("relationship", ""))
        grouped.setdefault(label, []).append(heir)

    # Reorder per RELATIONSHIP_GROUPS, then append 'Other Heirs' at the end
    ordered: dict[str, list[dict[str, Any]]] = {}
    for label, _ in RELATIONSHIP_GROUPS:
        if label in grouped:
            ordered[label] = grouped.pop(label)
    if "Other Heirs" in grouped:
        ordered["Other Heirs"] = grouped["Other Heirs"]
    return ordered


# ── Key findings (template) ──────────────────────────────────────────


def _money(value: str) -> str:
    if not value:
        return ""
    try:
        return f"${int(float(value)):,}"
    except (ValueError, TypeError):
        return value


def build_key_findings(notice: NoticeData) -> list[str]:
    """Build a list of plain-string bullets summarizing the notable facts."""
    bullets: list[str] = []

    # Equity position
    if notice.estimated_value and notice.estimated_equity:
        pct = f" ({notice.equity_percent}%)" if notice.equity_percent else ""
        bullets.append(
            f"Zestimate {_money(notice.estimated_value)} with roughly "
            f"{_money(notice.estimated_equity)}{pct} in equity."
        )
    elif notice.estimated_value:
        bullets.append(f"Zestimate {_money(notice.estimated_value)} — equity unknown.")

    # Tax delinquency
    if notice.tax_delinquent_amount:
        years = f" over {notice.tax_delinquent_years} year(s)" if notice.tax_delinquent_years else ""
        bullets.append(
            f"Tax delinquent: {_money(notice.tax_delinquent_amount)} owed{years}."
        )

    # MLS status
    status = (notice.mls_status or "").lower()
    if "active" in status or "for sale" in status:
        price = _money(notice.mls_listing_price) if notice.mls_listing_price else ""
        bullets.append(f"Currently listed on MLS{' at ' + price if price else ''}.")
    elif "pending" in status:
        bullets.append("Currently pending on MLS — move quickly or pass.")
    elif notice.mls_last_sold_date:
        last = _money(notice.mls_last_sold_price) if notice.mls_last_sold_price else ""
        bullets.append(
            f"Last sold {notice.mls_last_sold_date}"
            f"{' for ' + last if last else ''}."
        )

    # Auction / time pressure
    if notice.auction_date:
        nt = (notice.notice_type or "").replace("_", " ").title() or "Auction"
        bullets.append(f"{nt} auction scheduled for {notice.auction_date}.")

    # Signing chain size
    chain = (notice.signing_chain_count or "").strip()
    if chain and chain != "0":
        bullets.append(f"{chain} heir(s) hold signing authority and must agree to sell.")
    elif notice.decision_maker_name:
        bullets.append(
            f"Signing authority flows through {notice.decision_maker_name} "
            f"({(notice.decision_maker_relationship or 'heir').title()})."
        )

    # DM confidence
    conf = (notice.dm_confidence or "").lower()
    if conf == "low":
        bullets.append("Decision-maker identification is LOW confidence — verify before outreach.")
    elif conf == "medium":
        bullets.append("Decision-maker identification is medium confidence — supporting research recommended.")

    # Vacancy / deliverability
    if notice.vacant == "Y":
        bullets.append("Property appears VACANT (USPS vacancy flag) — likely lower contact rates at address.")

    return bullets


# ── Next steps (template) ────────────────────────────────────────────


def _first_phone(notice: NoticeData) -> str:
    for field in ("mobile_1", "mobile_2", "mobile_3", "mobile_4", "mobile_5",
                  "landline_1", "landline_2", "landline_3", "primary_phone"):
        val = getattr(notice, field, "") or ""
        if val.strip():
            return val.strip()
    return ""


def build_next_steps(notice: NoticeData) -> list[str]:
    """Build a list of recommended action bullets based on record state."""
    steps: list[str] = []

    dm_name = (notice.decision_maker_name or "").strip()
    dm_rel = (notice.decision_maker_relationship or "").strip()
    top_phone = _first_phone(notice)

    # 1. Start of the contact waterfall
    if dm_name and top_phone:
        who = f"{dm_name} ({dm_rel.title()})" if dm_rel else dm_name
        steps.append(f"Call {who} first at {top_phone}.")
    elif dm_name:
        steps.append(f"Skip-trace {dm_name} to locate a phone number before outreach.")
    else:
        steps.append("No primary decision maker identified — pull obituary and probate filings to find one.")

    # 2. Mailing address gap
    if dm_name and not (notice.decision_maker_street or "").strip():
        steps.append(
            f"Obtain a verified mailing address for {dm_name} "
            "(Tracerfy / people-search waterfall) before mail merge."
        )

    # 3. Confidence / verification
    conf = (notice.dm_confidence or "").lower()
    if conf in ("low", "medium"):
        steps.append(
            "Verify decision-maker identity against Knox County probate filings "
            "or obituary survivors list before committing marketing spend."
        )

    # 4. Signing chain guidance
    chain = (notice.signing_chain_count or "").strip()
    try:
        chain_n = int(chain) if chain else 0
    except ValueError:
        chain_n = 0
    if chain_n > 1:
        steps.append(
            f"Confirm all {chain_n} signing-authority heirs are aligned "
            "before drafting a purchase offer — any single dissent blocks the sale."
        )

    # 5. Tax situation
    if notice.tax_delinquent_amount:
        steps.append(
            "Pull the current tax ledger from the Knox County Trustee before offer — "
            "back taxes may need to roll into the purchase price."
        )

    # 6. Time pressure
    if notice.auction_date:
        steps.append(
            f"Auction on {notice.auction_date} is a hard deadline — "
            "prioritize this record ahead of non-time-sensitive leads."
        )

    # 7. DOD / obituary gap
    if not (notice.date_of_death or "").strip():
        steps.append(
            "Confirm date of death via obituary, SSDI, or death certificate "
            "to validate the probate trigger."
        )

    return steps


# ── Situation prose (LLM with template fallback) ─────────────────────


def _template_situation(notice: NoticeData) -> str:
    """Deterministic prose fallback when LLM is unavailable or fails."""
    parts: list[str] = []

    decedent = (notice.decedent_name or notice.owner_name or "The owner").strip()
    dod = (notice.date_of_death or "").strip()
    addr = (notice.address or "").strip()
    city = (notice.city or "").strip()
    if dod:
        parts.append(f"{decedent} passed away on {dod}.")
    else:
        parts.append(f"{decedent} is deceased (date of death unconfirmed).")
    if addr:
        loc = f"{addr}, {city}" if city else addr
        parts.append(f"Subject property: {loc}.")

    # Signing chain / DM
    dm = (notice.decision_maker_name or "").strip()
    dm_rel = (notice.decision_maker_relationship or "").strip()
    chain = (notice.signing_chain_count or "").strip()
    if dm:
        dm_str = f"{dm} ({dm_rel.title()})" if dm_rel else dm
        if chain and chain != "0":
            parts.append(
                f"{chain} living heir(s) must sign; primary decision maker is {dm_str}."
            )
        else:
            parts.append(f"Primary decision maker: {dm_str}.")
    else:
        parts.append("No primary decision maker has been identified yet.")

    # Equity headline
    if notice.estimated_value and notice.estimated_equity:
        pct = f" ({notice.equity_percent}%)" if notice.equity_percent else ""
        parts.append(
            f"Estimated value {_money(notice.estimated_value)} "
            f"with {_money(notice.estimated_equity)}{pct} equity."
        )

    # Time pressure
    if notice.auction_date:
        parts.append(f"Auction scheduled {notice.auction_date} — time-sensitive.")

    return " ".join(parts)


_SITUATION_SYSTEM = (
    "You are a senior real-estate-acquisitions analyst writing a concise "
    "situation summary for a deceased-owner deep-prospecting lead. "
    "Return strict JSON: {\"situation\": \"<2 to 3 sentence plain-English summary "
    "that an acquisitions manager can read in 10 seconds>\"}. "
    "Focus on who is deceased, who the decision maker is, property equity, "
    "signing-chain complexity, and any time pressure (auction, tax sale). "
    "Do not invent facts. Do not include markdown."
)


def _build_llm_facts(notice: NoticeData) -> dict[str, Any]:
    """Compact fact bundle for the LLM — only the fields that matter for a summary."""
    return {
        "decedent_name": notice.decedent_name or notice.owner_name,
        "date_of_death": notice.date_of_death,
        "property_address": f"{notice.address}, {notice.city}, {notice.state} {notice.zip}".strip(", "),
        "notice_type": notice.notice_type,
        "auction_date": notice.auction_date,
        "estimated_value": notice.estimated_value,
        "estimated_equity": notice.estimated_equity,
        "equity_percent": notice.equity_percent,
        "mls_status": notice.mls_status,
        "tax_delinquent_amount": notice.tax_delinquent_amount,
        "decision_maker_name": notice.decision_maker_name,
        "decision_maker_relationship": notice.decision_maker_relationship,
        "decision_maker_status": notice.decision_maker_status,
        "dm_confidence": notice.dm_confidence,
        "signing_chain_count": notice.signing_chain_count,
        "signing_chain_names": notice.signing_chain_names,
    }


def build_situation_prose(notice: NoticeData, api_key: str | None = None) -> str:
    """Generate a 2-3 sentence plain-English situation summary.

    Uses the LLM when an API key is available; falls back to a deterministic
    template on any failure so the PDF always renders.
    """
    key = api_key or getattr(config, "ANTHROPIC_API_KEY", "")
    if not key:
        return _template_situation(notice)

    facts = _build_llm_facts(notice)
    prompt = (
        "Summarize this deceased-owner real-estate lead in 2-3 sentences. "
        "Here are the known facts (JSON):\n\n"
        f"{json.dumps(facts, indent=2)}\n\n"
        "Return JSON with a single 'situation' key."
    )
    try:
        result = llm_client.chat_json(
            prompt=prompt,
            system=_SITUATION_SYSTEM,
            max_tokens=300,
            api_key=key,
        )
        if result and isinstance(result, dict):
            prose = (result.get("situation") or "").strip()
            if prose:
                return prose
    except Exception:
        logger.exception("LLM situation summary failed; falling back to template")

    return _template_situation(notice)
