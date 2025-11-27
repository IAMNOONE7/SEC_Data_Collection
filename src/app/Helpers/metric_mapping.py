from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional
from collections import defaultdict
from typing import Any, Dict

@dataclass
class MetricRule:
    """
    Definition of one canonical metric (e.g. 'Revenue') and rules
    how to find the best concept for it in a company's XBRL facts.
    """
    canonical_name: str
    # Explicit list of concepts we prefer in this order
    priority_concepts: List[str]
    # Fallback: if none of the priority concepts exist,
    # look for concepts whose *local name* contains any of these substrings.
    name_contains_any: List[str]

def _fact_to_dict(fact: Any) -> Dict[str, Any]:
    """
    Normalize a fact row to a dict.

    - If it's already a dict, return as-is
    - If it's a dataclass / object (e.g. FactRow), pull the fields we care about
    """
    if isinstance(fact, dict):
        return fact

    # Fall back to attribute access (works for dataclasses/NamedTuples/etc.)
    return {
        "concept": getattr(fact, "concept", None),
        "value": getattr(fact, "value", None),
        "period_start": getattr(fact, "period_start", None),
        "period_end": getattr(fact, "period_end", None),
        "instant": getattr(fact, "instant", None),
        "context_id": getattr(fact, "context_id", None),
    }

METRIC_RULES: List[MetricRule] = [
    MetricRule(
        canonical_name="Revenue",
        priority_concepts=[
            "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
            "us-gaap:Revenues",
            "us-gaap:SalesRevenueNet",
            "us-gaap:SalesRevenueGoodsNet",
            "us-gaap:SalesRevenueServicesNet",
        ],
        name_contains_any=["revenue", "sales"],
    ),
    MetricRule(
        canonical_name="GrossProfit",
        priority_concepts=[
            "us-gaap:GrossProfit",
            "us-gaap:GrossProfitIncludingDepreciation",
        ],
        name_contains_any=["grossprofit"],
    ),
    MetricRule(
        canonical_name="OperatingIncome",
        priority_concepts=[
            "us-gaap:OperatingIncomeLoss",
        ],
        name_contains_any=["operatingincome", "operatingloss"],
    ),
    MetricRule(
        canonical_name="NetIncome",
        priority_concepts=[
            "us-gaap:NetIncomeLoss",
            "us-gaap:ProfitLoss",
        ],
        name_contains_any=["netincome", "netincomeloss", "profitloss"],
    ),
    MetricRule(
        canonical_name="Assets",
        priority_concepts=[
            "us-gaap:Assets",
        ],
        name_contains_any=["assets"],
    ),
    MetricRule(
        canonical_name="LiabilitiesAndEquity",
        priority_concepts=[
            "us-gaap:LiabilitiesAndStockholdersEquity",
            "us-gaap:LiabilitiesAndPartnersCapital",
        ],
        name_contains_any=["liabilitiesandstockholdersequity", "liabilitiesandpartnerscapital"],
    ),
    MetricRule(
        canonical_name="CFO",  # cash flow from operations
        priority_concepts=[
            "us-gaap:NetCashProvidedByUsedInOperatingActivities",
        ],
        name_contains_any=["netcashprovidedbyusedinoperatingactivities"],
    ),
    MetricRule(
        canonical_name="CFI",
        priority_concepts=[
            "us-gaap:NetCashProvidedByUsedInInvestingActivities",
        ],
        name_contains_any=["netcashprovidedbyusedininvestingactivities"],
    ),
    MetricRule(
        canonical_name="CFF",
        priority_concepts=[
            "us-gaap:NetCashProvidedByUsedInFinancingActivities",
        ],
        name_contains_any=["netcashprovidedbyusedinfinancingactivities"],
    ),
    MetricRule(
        canonical_name="EPSBasic",
        priority_concepts=[
            "us-gaap:EarningsPerShareBasic",
        ],
        name_contains_any=["earningspersharebasic"],
    ),
    MetricRule(
        canonical_name="EPSDiluted",
        priority_concepts=[
            "us-gaap:EarningsPerShareDiluted",
        ],
        name_contains_any=["earningspersharediluted"],
    ),
]


def _build_concept_magnitude(facts: List[Dict]) -> Dict[str, float]:
    """
    From a list of flattened facts, build a dict:

        concept -> max(|numeric value|) across all its facts.

    Non-numeric facts are ignored.
    """
    concept_mag: Dict[str, float] = {}

    for f in facts:
        fd = _fact_to_dict(f)

        concept = fd.get("concept")
        v_raw = fd.get("value")
        if concept is None:
            continue

        # your existing parsing logic…
        try:
            v_num = float(v_raw)
        except (TypeError, ValueError):
            continue

        concept_mag[concept] = concept_mag.get(concept, 0.0) + abs(v_num)

    return concept_mag


def _choose_concept_for_rule(
    rule: MetricRule,
    concept_mag: Dict[str, float],
) -> Optional[str]:
    """
    Pick the best concept for a single MetricRule.
    1) If any priority_concepts exist, return the first one present.
    2) Otherwise, search by name_contains_any and pick the one with
       the largest magnitude.
    """
    # Step 1: explicit priority list
    for c in rule.priority_concepts:
        if c in concept_mag:
            return c

    # Step 2: name-based fuzzy search
    best: Optional[str] = None
    best_mag: float = 0.0

    for concept, mag in concept_mag.items():
        local = concept.split(":", 1)[-1].lower()  # "NetIncomeLoss" → "netincomeloss"
        if any(pattern in local for pattern in rule.name_contains_any):
            if mag > best_mag:
                best_mag = mag
                best = concept

    return best


def build_metric_map_for_company(facts: List[Dict]) -> Dict[str, str]:
    """
    Given a list of consolidated facts for ONE company (e.g. all rows from
    extract_company_totals_for_main_period), automatically build:

        {
          "Revenue": "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
          "NetIncome": "us-gaap:NetIncomeLoss",
          ...
        }

    Only metrics we can confidently detect will appear in the result.
    """
    concept_mag = _build_concept_magnitude(facts)
    result: Dict[str, str] = {}

    for rule in METRIC_RULES:
        chosen = _choose_concept_for_rule(rule, concept_mag)
        if chosen:
            result[rule.canonical_name] = chosen

    return result