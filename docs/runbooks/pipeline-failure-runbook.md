# Runbook: Data Pipeline Failure

**Audience:** Data Engineering on-call  
**Last Updated:** 2024-01-20  
**Escalation:** `#data-platform` Slack → `platform-ops@contoso.example`

---

## 1. Initial Triage

When a pipeline failure alert fires in Azure Monitor:

1. Open the [Fabric Workspace Monitor](https://app.fabric.microsoft.com) and navigate to **Monitor → Pipeline Runs**.
2. Identify the failing pipeline and click into the run to see the failed activity.
3. Copy the `Run ID` from the run details (you will need this for log queries).
4. Check the activity error message and error code.

Common error codes:

| Error Code           | Likely Cause                                          | First Action               |
|----------------------|-------------------------------------------------------|----------------------------|
| `SqlErrorCode_2601`  | Duplicate key violation in control table              | Re-run the pipeline        |
| `SILVER_NOT_READY`   | Gold pipeline ran before silver completed             | Check silver run status    |
| `DQ_CHECK_FAILED`    | Data quality check failed on ingested batch           | See section 3              |
| `ConnectionTimeout`  | Source system unavailable                             | Check source system health |
| `CapacityExceeded`   | Fabric CU capacity hit                                | Wait or scale up           |

---

## 2. Checking Logs

Query Log Analytics for pipeline run details:

```kusto
// Pipeline runs in the last 24h
FabricOperationsLogs
| where TimeGenerated > ago(24h)
| where OperationName == "PipelineRun"
| where Status == "Failed"
| project TimeGenerated, PipelineName, RunId, ActivityName, ErrorMessage, Duration
| order by TimeGenerated desc
```

Query the SQL control table directly:

```sql
SELECT TOP 20
    pipeline_name,
    run_id,
    status,
    rows_processed,
    last_watermark,
    run_date,
    error_message
FROM dbo.pipeline_control
WHERE status = 'FAILED'
ORDER BY run_date DESC;
```

---

## 3. Data Quality Failures

If the pipeline fails at `RunDataQualityCheck`:

1. Open the corresponding DQ notebook (`nb_dq_bronze_<table>.ipynb`) in the Fabric workspace.
2. Run the notebook manually with the failing `run_id` as a parameter.
3. Review the DQ report — each check shows pass/fail and row counts.
4. If the failure is a legitimate data issue (e.g. nulls in a required field), escalate to the source system team.
5. If the failure is a transient/false-positive issue, re-run the pipeline with `skip_dq=true` (requires data engineering lead approval).

---

## 4. Re-running a Failed Pipeline

**Via Fabric UI:**
1. Go to **Monitor → Pipeline Runs**
2. Find the failed run
3. Click **Rerun** (this uses the same parameters as the original run)

**Via Azure CLI / script:**
```bash
# Trigger manual re-run of the ingest pipeline for a specific watermark window
python scripts/trigger_pipeline.py \
  --pipeline ingest_sales_orders \
  --workspace-id "$FABRIC_WORKSPACE_ID" \
  --param watermark_start="2024-01-23 00:00:00" \
  --param watermark_end="2024-01-24 00:00:00"
```

---

## 5. Watermark Recovery

If the watermark table is in an inconsistent state:

```sql
-- 1. Check current watermark
SELECT * FROM dbo.pipeline_control
WHERE pipeline_name = 'ingest_sales_orders'
ORDER BY run_date DESC;

-- 2. Manually correct the watermark (requires DBA approval in prod)
UPDATE dbo.pipeline_control
SET last_watermark = '2024-01-22 00:00:00',
    status         = 'SUCCESS',
    error_message  = NULL
WHERE pipeline_name = 'ingest_sales_orders'
  AND status        = 'FAILED'
  AND run_date      = '2024-01-23';
```

> **⚠️ WARNING:** Manual watermark changes in production require approval from the Data Engineering Lead. Document the change in the incident ticket.

---

## 5. Escalation Path

| Severity | Condition                                       | Action                                          |
|----------|-------------------------------------------------|-------------------------------------------------|
| P1       | Gold pipeline failed, Power BI reports stale    | Page on-call lead immediately                   |
| P2       | Silver pipeline delayed > 2h                    | Notify `#data-platform`, investigate            |
| P3       | Bronze pipeline delayed, silver not yet impacted| Investigate during business hours               |
| P4       | Single DQ warning (not blocking)                | Log ticket, investigate next business day       |
