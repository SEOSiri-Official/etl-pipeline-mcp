# tests/test_etl_pipeline.py
import json
import os
import sys
import sqlite3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.main_server import (
    extract_realtime_stream,
    poll_crm_batch,
    transform_and_stitch_batch,
    export_to_data_warehouse,
    export_to_parquet_buffer,
    get_pipeline_analytics_summary,
    get_live_throughput_metrics,
    ingest_hubspot_webhook,
    ingest_universal_event,
    COLD_DB_PATH
)

def setup_function():
    """Wipes cold storage before each test for clean isolation."""
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

def test_1_extract_realtime_stream():
    payload = json.dumps({"email": "momenul@seosiri.com", "event": "click"})
    res = json.loads(extract_realtime_stream("TWITTER", payload))
    assert res["status"] == "INGESTED_TO_HOT_TIER"

def test_2_poll_crm_batch():
    payload = json.dumps({"crm_lead_id": "hs_100", "email": "momenul@seosiri.com"})
    res = json.loads(poll_crm_batch("HUBSPOT", "hs_100", "momenul@seosiri.com", payload))
    assert res["status"] == "BATCH_SYNC_SUCCESS"

def test_3_ingest_hubspot_webhook():
    webhook_data = json.dumps([{
        "eventId": 1001,
        "subscriptionType": "contact.creation",
        "objectId": 12345,
        "portalId": 3271531
    }])
    res = json.loads(ingest_hubspot_webhook(webhook_data))
    assert res["status"] == "SUCCESS"
    assert res["events_ingested"] == 1

def test_4_transform_and_stitch_batch():
    payload = json.dumps({"email": "momenul@seosiri.com", "crm_id": "hs_100"})
    extract_realtime_stream("HUBSPOT", payload)
    res = json.loads(transform_and_stitch_batch(max_batch_size=10))
    assert res["status"] == "TRANSFORMATION_COMPLETE"

def test_5_pipeline_analytics_summary():
    res = json.loads(get_pipeline_analytics_summary("ALL"))
    assert res["status"] == "ANALYTICS_RESOLVED"

def test_6_live_throughput_metrics():
    res = json.loads(get_live_throughput_metrics())
    assert res["system_health"] == "HEALTHY"

def test_7_export_to_parquet_buffer():
    res = json.loads(export_to_parquet_buffer(limit=10))
    assert res["status"] == "PARQUET_BUFFER_GENERATED"

def test_8_ingest_universal_event():
    stripe_payload = json.dumps({"customer_id": "cus_123", "amount": 5000, "currency": "usd"})
    res = json.loads(ingest_universal_event("STRIPE", "payment_intent.succeeded", stripe_payload))
    assert res["status"] == "INGESTED_TO_HOT_TIER"
    assert res["source"] == "STRIPE"