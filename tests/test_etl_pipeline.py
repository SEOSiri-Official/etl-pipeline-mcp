# tests/test_etl_pipeline.py
import json
import os
import sys
import sqlite3

# Force project root into Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.main_server import (
    extract_realtime_stream,
    poll_crm_batch,
    transform_and_stitch_batch,
    export_to_data_warehouse,
    get_pipeline_analytics_summary,
    COLD_DB_PATH
)

def test_full_mcp_etl_lifecycle():
    # 1. Clean up local test database before starting
    if os.path.exists(COLD_DB_PATH):
        conn = sqlite3.connect(COLD_DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM cold_archive")
            cursor.execute("DELETE FROM identity_registry")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()

    # 2. EXTRACT: Ingest a real-time event into the Hot Tier
    webhook_payload = json.dumps({
        "email": "momenul@seosiri.com",
        "social_id": "twitter_momenul",
        "event": "conversion"
    })
    res_1 = json.loads(extract_realtime_stream("TWITTER", webhook_payload))
    assert res_1["status"] == "INGESTED_TO_HOT_TIER"
    assert res_1["queue_size"] == 1

    # 3. EXTRACT: Poll a batch record from a CRM (HubSpot) straight to Cold Tier
    crm_payload = json.dumps({
        "crm_lead_id": "hubspot_999",
        "email": "momenul@seosiri.com",
        "company": "SEOSiri"
    })
    res_2 = json.loads(poll_crm_batch("HUBSPOT", "hubspot_999", "momenul@seosiri.com", crm_payload))
    assert res_2["status"] == "BATCH_SYNC_SUCCESS"

    # 4. TRANSFORM: Process the Hot Tier queue, stitch identities, and redact PII
    res_3 = json.loads(transform_and_stitch_batch(max_batch_size=10))
    assert res_3["status"] == "TRANSFORMATION_COMPLETE"
    assert res_3["records_migrated"] == 1

    # 5. ANALYTICS: Verify that both records exist in Cold Storage under 1 unified identity
    res_4 = json.loads(get_pipeline_analytics_summary("ALL"))
    summary = res_4
    assert summary["hot_tier_pending_events"] == 0
    assert summary["cold_tier_archived_records"] == 2
    assert summary["total_unique_identities_stitched"] == 1

    # 6. LOAD: Export to Snowflake Data Warehouse
    res_5 = json.loads(export_to_data_warehouse("SNOWFLAKE", limit=10))
    assert res_5["status"] == "LOAD_SUCCESSFUL"
    assert res_5["target_warehouse"] == "SNOWFLAKE"
    assert res_5["records_exported"] == 2
    
    # Verify PII (email) is redacted in exported data
    exported_data_str = str(res_5["exported_buffer"])
    assert "momenul@seosiri.com" not in exported_data_str