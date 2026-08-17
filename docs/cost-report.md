# Cost report

This project was built and validated using Azure's free trial credit ($200 / 30 days). Resources were deleted after evidence capture to avoid ongoing charges.

**Confirmed total spend: $0.00** — see `docs/screenshots/06_cost_management.png` for the Azure Cost Management dashboard showing this directly.

| Service | Cost driver | Actual cost |
|---|---|---|
| ADLS Gen2 storage (`healthcarelakehouse01`) | ~650MB stored for a few days | $0.00 |
| Azure Databricks (Serverless) | Compute seconds during Bronze/Silver/Gold development and the final validated run (3m 21s) | $0.00 |
| Azure Data Factory | Pipeline debug/test runs | $0.00 |

## Why this stayed at $0

- Serverless-only workspace means there's no persistent cluster that could be left running idle by accident — the most common source of unexpected Databricks cost.
- All development and debugging (including fixing the `claims.csv` foreign key bug and the ADF cluster-compatibility issue) happened within Azure's free trial credit allowance.
- Resource group deleted in a single operation immediately after evidence capture, removing storage, Databricks workspace, and Data Factory together — no resources left running post-validation.
