# Pipeline Fixes - Root Cause Analysis & Implementation Plan

## ROOT CAUSE ANALYSIS

### Bug #1: Pipeline and API are DISCONNECTED (Critical)
- `pipeline/pipeline_runner.py` writes events to `data/events.jsonl`
- `app/` reads from `store_intelligence.db` via SQLite
- **No code exists** that reads `events.jsonl` and ingests into the database
- `scripts/replay_events.py` exists but must be run MANUALLY
- **Result**: Pipeline produces 50 events in JSONL, DB has only 1 event

### Bug #2: Pipeline container never executes (Critical)
- `docker-compose.yml` line 55-57: `command: ["sleep", "infinity"]`
- Pipeline container just sleeps forever, never processes videos
- **Result**: No pipeline data ever enters the system

### Bug #3: Time window mismatch (Critical)
- `MetricsService` and `DashboardSummaryService` always query TODAY's window
- Pipeline events have timestamps from June 4-19, 2026
- **Result**: Even with data in DB, dashboard shows zeros (wrong time window)

### Bug #4: No automated ingestion bridge (Critical)
- Pipeline output (`events.jsonl`) has different schema from API ingestion endpoint
- No automated mechanism to bridge pipeline output → API database
- **Result**: Pipeline completes successfully but data is stranded in JSONL files

### Bug #5: `generated_events.jsonl` has 29 events, none in DB
- `data/generated_events.jsonl` has CAM_5 billing camera zone events
- These events exist on disk but were never ingested into the database

### Bug #6: Dashboard lacks auto-refresh with data awareness
- Auto-refresh loops but doesn't adapt polling to data freshness
- Falls through to empty states immediately without diagnostic info

## IMPLEMENTATION FIXES

1. Create `pipeline/ingest_bridge.py` - Automated pipeline output → DB ingestion
2. Fix `docker-compose.yml` - Pipeline runs on startup, auto-ingests
3. Fix `app/metrics.py` - Support querying past timestamps, auto-detect data range
4. Fix `app/dashboard_summary.py` - Support historical data
5. Fix `dashboard/streamlit_app.py` - Better diagnostics, handle no-data gracefully
6. Add `Dockerfile` fixes for proper pipeline execution
7. Add `requirements-pipeline.txt` fixes