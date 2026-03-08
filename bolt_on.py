"""
bolt_on.py — Bolt-on opportunity analysis

After all companies in a sector are enriched, this module:
1. Clusters companies by SIC code and service keywords
2. Identifies service adjacencies across the sector
3. Scores cross-selling and consolidation opportunities
4. Produces a bolt-on recommendation table

Output is saved to output/bolt_on_analysis.json and added to the Excel.
"""

import json
import os
from collections import Counter, defaultdict

import config as cfg


def analyse(companies: list[dict]) -> dict:
    """
    Generate bolt-on analysis for a list of enriched companies.

    Returns:
        {
          "sic_clusters": {...},
          "service_adjacencies": [...],
          "bolt_on_recommendations": [...],
          "market_fragmentation": {...},
        }
    """

    # ── SIC clustering ────────────────────────────────────────────────────────
    sic_counter = Counter()
    sic_companies = defaultdict(list)
    for c in companies:
        for sic in c.get("sic_codes", []):
            sic_counter[sic] += 1
            sic_companies[sic].append(c["company_name"])

    sic_clusters = [
        {
            "sic_code":     sic,
            "company_count": count,
            "example_companies": sic_companies[sic][:5],
        }
        for sic, count in sic_counter.most_common(10)
    ]

    # ── Service adjacency (from website keywords where available) ─────────────
    all_services = []
    for c in companies:
        contacts = c.get("contacts", {})
        site_emails = contacts.get("site_emails", [])
        # Use company name keywords as proxy for services when website data absent
        name_words = [
            w.lower() for w in c["company_name"].split()
            if len(w) > 4 and w.lower() not in (
                "limited", "services", "solutions", "group", "systems",
                "engineering", "technical", "consulting", "management"
            )
        ]
        all_services.extend(name_words)

    service_freq = Counter(all_services).most_common(20)

    # ── Market fragmentation index ────────────────────────────────────────────
    # Fragmentation Index = Number of Companies / (estimated top 5 market share)
    # For SME sectors, we use number of companies as a proxy
    total_cos = len(companies)
    top5_share_est = 0.25  # estimated — configurable per sector
    fragmentation_index = round(total_cos / (top5_share_est * 100), 2)

    fragmentation = {
        "total_companies_analysed": total_cos,
        "estimated_top5_market_share": f"{top5_share_est*100:.0f}%",
        "fragmentation_index":     fragmentation_index,
        "formula":  "Companies Analysed / (Top 5 Market Share × 100)",
        "interpretation": (
            "Highly fragmented — strong roll-up potential" if fragmentation_index > 3
            else "Moderately fragmented" if fragmentation_index > 1.5
            else "Concentrated — limited roll-up opportunity"
        ),
    }

    # ── Bolt-on recommendations ───────────────────────────────────────────────
    # Derived from sector config + SIC adjacencies
    bolt_on_recs = _generate_recommendations(sic_clusters, companies)

    return {
        "sic_clusters":            sic_clusters,
        "service_frequency":       [{"term": t, "count": c} for t, c in service_freq],
        "market_fragmentation":    fragmentation,
        "bolt_on_recommendations": bolt_on_recs,
    }


def _generate_recommendations(sic_clusters: list[dict],
                               companies: list[dict]) -> list[dict]:
    """
    Generate bolt-on recommendations using SIC adjacency logic.
    Each recommendation identifies a service cluster and example targets.
    """
    # Build adjacency map: companies per SIC sorted by acquisition score
    sic_to_targets = defaultdict(list)
    for c in companies:
        score = c.get("acquisition_score", 0)
        for sic in c.get("sic_codes", []):
            sic_to_targets[sic].append({
                "name":  c["company_name"],
                "number": c["company_number"],
                "score": score,
            })

    # Sort targets within each SIC by score
    for sic in sic_to_targets:
        sic_to_targets[sic].sort(key=lambda x: x["score"], reverse=True)

    # Use sector config adjacency hints if defined
    adjacencies = getattr(cfg, "BOLT_ON_ADJACENCIES", _default_adjacencies())

    recs = []
    for adj in adjacencies:
        example_targets = []
        for sic in adj.get("sic_codes", []):
            example_targets.extend(sic_to_targets.get(sic, [])[:3])
        # Deduplicate
        seen = set()
        unique_targets = []
        for t in example_targets:
            if t["name"] not in seen:
                seen.add(t["name"])
                unique_targets.append(t)

        recs.append({
            "cluster":              adj["cluster"],
            "rationale":            adj["rationale"],
            "bolt_on_services":     adj["bolt_on_services"],
            "target_sic_codes":     adj.get("sic_codes", []),
            "example_targets":      unique_targets[:5],
            "opportunity_score":    adj.get("opportunity_score", 5),
        })

    return sorted(recs, key=lambda x: x["opportunity_score"], reverse=True)


def _default_adjacencies() -> list[dict]:
    """
    Generic adjacency map used when sector config does not define one.
    """
    return [
        {
            "cluster":         "Measurement & Testing Services",
            "rationale":       "Natural extension of calibration — same customer base, similar equipment",
            "bolt_on_services":["dimensional inspection", "non-destructive testing", "environmental testing"],
            "sic_codes":       ["71200", "71122"],
            "opportunity_score": 9,
        },
        {
            "cluster":         "Instrument Repair & Maintenance",
            "rationale":       "Recurring revenue stream; customers already own calibrated equipment",
            "bolt_on_services":["instrument repair", "preventive maintenance", "asset management"],
            "sic_codes":       ["33130", "33190", "33140"],
            "opportunity_score": 8,
        },
        {
            "cluster":         "Compliance & Quality Consulting",
            "rationale":       "Higher margin advisory layer on top of technical services",
            "bolt_on_services":["ISO 17025 consulting", "quality systems", "UKAS accreditation support"],
            "sic_codes":       ["74909", "71122"],
            "opportunity_score": 7,
        },
    ]


def run():
    enriched_path = os.path.join(cfg.OUTPUT_DIR, cfg.ENRICHED_JSON)
    with open(enriched_path) as f:
        companies = json.load(f)

    print(f"\nGenerating bolt-on analysis for {len(companies)} companies...")
    result = analyse(companies)

    out_path = os.path.join(cfg.OUTPUT_DIR, "bolt_on_analysis.json")
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Done → {out_path}")
    print(f"  Market fragmentation index: {result['market_fragmentation']['fragmentation_index']}")
    print(f"  Bolt-on clusters identified: {len(result['bolt_on_recommendations'])}")
    return result


if __name__ == "__main__":
    run()
