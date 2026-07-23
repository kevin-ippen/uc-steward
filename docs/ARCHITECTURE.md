# Architecture & Positioning

## Where UC Steward Sits

| | Governance Hub | UC Steward |
|---|---|---|
| **Question** | Where is our posture weak and what should we prioritize? | How do we correct this without breaking consumers, and how do we prove it's resolved? |
| **Mode** | Bulk-safe: tags, classification, metadata quality at scale | Risky-per-asset: lineage scoping + compatibility layer + rollback |

**The boundary**: safe-in-bulk versus risky-per-asset. These don't converge.

## Design Principles

1. **Propose, don't author.** The metadata enricher proposes metric-view YAML, glossary candidates, and governed tags for approval. It doesn't write free-text descriptions everywhere.

2. **Native is the system of record.** Tags, domains, certification status — these are platform-native. UC Steward detects exceptions, handles policy conflicts, and stores approval evidence. It retires custom vocabulary management as native coverage matures.

3. **Bridge views are the unit of safe change.** The bridge view transfers outage risk from the person to the tool. TTL management, consumer migration tracking, and external-consumer-driven risk-adjusted TTLs are where marginal effort pays best.

4. **Certification lifecycle, not just status.** Native certification is a status field with no lifecycle. UC Steward adds: expiry, evidence, escalation, review cadence. The output is a `system.certification_status` write after an approved transition.

5. **Accept external prioritization.** Findings intake as a contract, not a scanner-only API. Other tools can submit violations; UC Steward handles the remediation workflow.

## What We Build

- Lineage-aware remediation (rename + bridge view + migration tracking)
- Semantic drift → owner review (invalidation routing)
- Decertification detection (certified + drifted/stale)
- Cost attribution for stale assets
- Domain assignment proposals (reads native, suggests, doesn't own)
- Bridge view deepening (TTL, consumer tracking, external registry)

## What We Don't Build

- Custom tag vocabulary management (native is SOR)
- Raw tag propagation at scale (bulk-safe = Hub)
- Generic posture dashboards (Hub's surface)
- A parallel domain-ownership table (Domains is native)
- Anything that races the platform's certification lifecycle

## What We Stop When Native Ships

- Custom tag propagation → native tag governance
- Metadata quality scoring → Hub's coverage metrics
- Bulk classification → native classification scanning
