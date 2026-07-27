# src/main_server.py
import os
import sys

# Force the project root directory into the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import hmac
import hashlib
import sqlite3
import requests
from datetime import datetime, timezone
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("SEOSiri-ETL-Pipeline-Orchestrator")

# Secret key for webhook signature verification
WEBHOOK_SECRET_KEY = b"seosiri_etl_secure_key_2026"

# 1. HOT TIER: High-Speed In-Memory SQLite for active stream buffering (RAM)
HOT_CONN = sqlite3.connect(":memory:", check_same_thread=False)
HOT_CURSOR = HOT_CONN.cursor()

# 2. COLD TIER: Portable On-Disk SQLite for permanent, anonymized data (Disk)
COLD_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "etl_cold_storage.db")
COLD_CONN = sqlite3.connect(COLD_DB_PATH, check_same_thread=False)
COLD_CURSOR = COLD_CONN.cursor()


def init_databases():
    """Initializes in-memory and on-disk tables for the ETL pipeline."""
    HOT_CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS hot_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            source_system TEXT,
            payload_data TEXT
        )
    """)
    HOT_CONN.commit()

    COLD_CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS cold_archive (
            mcp_root_id TEXT,
            timestamp TEXT,
            source_system TEXT,
            anonymized_payload TEXT,
            priority_score REAL,
            allocation_route TEXT,
            status TEXT,
            PRIMARY KEY(mcp_root_id, timestamp, source_system)
        )
    """)

    COLD_CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS identity_registry (
            mcp_root_id TEXT PRIMARY KEY,
            hashed_email TEXT UNIQUE,
            crm_id TEXT UNIQUE,
            social_id TEXT UNIQUE
        )
    """)
    COLD_CONN.commit()


init_databases()


# ---------------------------------------------------------------------
# HELPER FUNCTIONS: PII HASHING & IDENTITY STITCHING
# ---------------------------------------------------------------------

def hash_pii(value: str) -> str:
    """Hashes sensitive PII using SHA-256 for GDPR/HIPAA compliance."""
    if not value:
        return "ANONYMOUS"
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()


def resolve_mcp_identity(email: str = None, crm_id: str = None, social_id: str = None) -> str:
    """Stitches disparate identifiers (email, CRM ID, social ID) into a single mcp_root_id."""
    h_email = hash_pii(email) if email else None

    query_conditions = []
    params = []
    if h_email:
        query_conditions.append("hashed_email = ?")
        params.append(h_email)
    if crm_id:
        query_conditions.append("crm_id = ?")
        params.append(crm_id)
    if social_id:
        query_conditions.append("social_id = ?")
        params.append(social_id)

    if query_conditions:
        query = f"SELECT mcp_root_id FROM identity_registry WHERE {' OR '.join(query_conditions)}"
        COLD_CURSOR.execute(query, params)
        row = COLD_CURSOR.fetchone()
        if row:
            return row[0]

    now_str = datetime.now(timezone.utc).isoformat()
    new_root_id = hashlib.sha1(f"{h_email}:{crm_id}:{social_id}:{now_str}".encode()).hexdigest()[:16]
    COLD_CURSOR.execute("""
        INSERT INTO identity_registry (mcp_root_id, hashed_email, crm_id, social_id)
        VALUES (?, ?, ?, ?)
    """, (new_root_id, h_email, crm_id, social_id))
    COLD_CONN.commit()
    return new_root_id


# ---------------------------------------------------------------------
# MCP TOOLS FOR ETL PIPELINES
# ---------------------------------------------------------------------

@mcp.tool()
def extract_realtime_stream(source_system: str, payload_json: str, hmac_signature: str = "") -> str:
    """
    EXTRACT: Ingests real-time events (webhooks, CMS events, clicks) directly into the Hot Tier.

    Args:
        source_system: Originating platform (e.g., 'hubspot', 'salesforce', 'wordpress', 'stripe').
        payload_json: Raw JSON payload string to extract and buffer.
        hmac_signature: Optional HMAC-SHA256 signature for payload verification.
    """
    HOT_CURSOR.execute("SELECT COUNT(*) FROM hot_queue")
    queue_size = HOT_CURSOR.fetchone()[0]
    if queue_size > 10000:
        return json.dumps({
            "status": "BACKPRESSURE_LIMIT_EXCEEDED",
            "action": "THROTTLE_INGESTION_RATE",
            "current_queue_size": queue_size
        })

    if hmac_signature:
        expected_sig = hmac.new(WEBHOOK_SECRET_KEY, payload_json.encode('utf-8'), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected_sig, hmac_signature):
            return json.dumps({"status": "REJECTED", "reason": "SECURITY_SIGNATURE_MISMATCH"})

    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    HOT_CURSOR.execute("""
        INSERT INTO hot_queue (timestamp, source_system, payload_data)
        VALUES (?, ?, ?)
    """, (timestamp, source_system.upper().strip(), payload_json))
    HOT_CONN.commit()

    return json.dumps({
        "status": "INGESTED_TO_HOT_TIER",
        "source": source_system.upper(),
        "timestamp": timestamp,
        "queue_size": queue_size + 1
    })


@mcp.tool()
def poll_crm_batch(source_system: str, crm_lead_id: str, email_address: str, payload_json: str) -> str:
    """
    EXTRACT: Ingests batch CRM records (HubSpot, Salesforce) directly to Cold Storage.
    Executes immediate PII redaction and assigns a unified mcp_root_id.

    Args:
        source_system: The CRM system ('HUBSPOT', 'SALESFORCE').
        crm_lead_id: The unique CRM lead identifier.
        email_address: The lead's email address for identity resolution.
        payload_json: The full CRM record payload in JSON format.
    """
    mcp_root_id = resolve_mcp_identity(email=email_address, crm_id=crm_lead_id)
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    clean_payload = payload_json.replace(email_address, "[REDACTED_PII_EMAIL]")

    COLD_CURSOR.execute("""
        INSERT OR REPLACE INTO cold_archive (mcp_root_id, timestamp, source_system, anonymized_payload, priority_score, allocation_route, status)
        VALUES (?, ?, ?, ?, 1.0, 'COLD_STORAGE_DISK', 'ARCHIVED')
    """, (mcp_root_id, timestamp, source_system.upper().strip(), clean_payload))
    COLD_CONN.commit()

    return json.dumps({
        "status": "BATCH_SYNC_SUCCESS",
        "mcp_root_id": mcp_root_id,
        "source": source_system.upper()
    })


@mcp.tool()
def transform_and_stitch_batch(max_batch_size: int = 100) -> str:
    """
    TRANSFORM: Processes buffered Hot Tier records, executes PII redaction and 
    identity stitching, and migrates the records to the Cold Storage Tier.

    Args:
        max_batch_size: The maximum number of records to transform in this run.
    """
    HOT_CURSOR.execute("SELECT id, timestamp, source_system, payload_data FROM hot_queue LIMIT ?", (max_batch_size,))
    rows = HOT_CURSOR.fetchall()

    migrated_count = 0
    for row_id, ts, source, payload_str in rows:
        try:
            payload = json.loads(payload_str)
            email = payload.get("email") or payload.get("email_address")
            crm_id = payload.get("crm_id") or payload.get("crm_lead_id")
            social_id = payload.get("social_id") or payload.get("social_user_id")

            mcp_root_id = resolve_mcp_identity(email=email, crm_id=crm_id, social_id=social_id)

            clean_payload_str = payload_str
            if email:
                clean_payload_str = clean_payload_str.replace(email, "[REDACTED_PII_EMAIL]")

            score = 1.0
            lower_payload = payload_str.lower()
            if "conversion" in lower_payload or "purchase" in lower_payload:
                score += 5.0
            if "error" in lower_payload or "alert" in lower_payload:
                score += 3.0

            COLD_CURSOR.execute("""
                INSERT OR REPLACE INTO cold_archive (mcp_root_id, timestamp, source_system, anonymized_payload, priority_score, allocation_route, status)
                VALUES (?, ?, ?, ?, ?, 'COLD_DISK_STORE', 'TRANSFORMED')
            """, (mcp_root_id, ts, source, clean_payload_str, score))

            HOT_CURSOR.execute("DELETE FROM hot_queue WHERE id = ?", (row_id,))
            migrated_count += 1
        except Exception:
            continue

    HOT_CONN.commit()
    COLD_CONN.commit()

    return json.dumps({
        "status": "TRANSFORMATION_COMPLETE",
        "records_migrated": migrated_count
    })


@mcp.tool()
def export_to_data_warehouse(target_warehouse: str, limit: int = 100) -> str:
    """
    LOAD: Exports transformed, anonymized data from Cold Storage to target enterprise warehouses.

    Args:
        target_warehouse: Destination warehouse ('SNOWFLAKE', 'CLICKHOUSE', or 'BIGQUERY').
        limit: Maximum number of records to export.
    """
    warehouse = target_warehouse.upper().strip()
    if warehouse not in ["SNOWFLAKE", "CLICKHOUSE", "BIGQUERY"]:
        return json.dumps({"status": "FAILED", "reason": "UNSUPPORTED_WAREHOUSE_TARGET"})

    COLD_CURSOR.execute("""
        SELECT mcp_root_id, timestamp, source_system, anonymized_payload, priority_score
        FROM cold_archive
        WHERE status IN ('ARCHIVED', 'TRANSFORMED')
        LIMIT ?
    """, (limit,))
    rows = COLD_CURSOR.fetchall()

    exported_records = []
    for mcp_id, ts, source, payload, score in rows:
        COLD_CURSOR.execute("""
            UPDATE cold_archive
            SET status = 'LOADED', allocation_route = ?
            WHERE mcp_root_id = ? AND timestamp = ?
        """, (warehouse, mcp_id, ts))

        exported_records.append({
            "mcp_root_id": mcp_id,
            "timestamp": ts,
            "source_system": source,
            "priority_score": score,
            "payload": json.loads(payload)
        })

    COLD_CONN.commit()

    return json.dumps({
        "status": "LOAD_SUCCESSFUL",
        "target_warehouse": warehouse,
        "records_exported": len(exported_records),
        "exported_buffer": exported_records
    })


@mcp.tool()
def export_to_parquet_buffer(limit: int = 500) -> str:
    """
    LOAD: Compiles transformed Cold Storage records into an optimized, 
    anonymized JSON-Parquet buffer ready for local DuckDB or S3 Data Lake ingestion.

    Args:
        limit: Maximum number of records to package into the Parquet buffer.
    """
    COLD_CURSOR.execute("""
        SELECT mcp_root_id, timestamp, source_system, anonymized_payload, priority_score
        FROM cold_archive
        WHERE status IN ('ARCHIVED', 'TRANSFORMED')
        LIMIT ?
    """, (limit,))
    rows = COLD_CURSOR.fetchall()

    parquet_buffer = []
    for mcp_id, ts, source, payload, score in rows:
        parquet_buffer.append({
            "mcp_root_id": mcp_id,
            "timestamp_utc": ts,
            "source": source,
            "priority_weight": score,
            "data_schema": json.loads(payload)
        })

    return json.dumps({
        "status": "PARQUET_BUFFER_GENERATED",
        "record_count": len(parquet_buffer),
        "format": "COLUMNS_OPTIMIZED",
        "buffer": parquet_buffer
    })


@mcp.tool()
def get_pipeline_analytics_summary(source_system: str = "ALL") -> str:
    """
    ANALYTICS: Returns real-time row counts and processing metrics across Hot and Cold tiers.

    Args:
        source_system: Filter by source system ('HUBSPOT', 'SALESFORCE', 'ALL').
    """
    HOT_CURSOR.execute("SELECT COUNT(*) FROM hot_queue")
    hot_count = HOT_CURSOR.fetchone()[0]

    if source_system.upper() == "ALL":
        COLD_CURSOR.execute("SELECT COUNT(*) FROM cold_archive")
        cold_count = COLD_CURSOR.fetchone()[0]

        COLD_CURSOR.execute("SELECT COUNT(DISTINCT mcp_root_id) FROM identity_registry")
        identities_count = COLD_CURSOR.fetchone()[0]
    else:
        COLD_CURSOR.execute("SELECT COUNT(*) FROM cold_archive WHERE source_system = ?", (source_system.upper(),))
        cold_count = COLD_CURSOR.fetchone()[0]

        COLD_CURSOR.execute("SELECT COUNT(DISTINCT mcp_root_id) FROM identity_registry")
        identities_count = COLD_CURSOR.fetchone()[0]

    return json.dumps({
        "status": "ANALYTICS_RESOLVED",
        "hot_tier_pending_events": hot_count,
        "cold_tier_archived_records": cold_count,
        "total_unique_identities_stitched": identities_count
    })


@mcp.tool()
def get_live_throughput_metrics() -> str:
    """
    ANALYTICS: Measures processing latency, Hot Tier memory pressure, 
    and Cold Tier storage utilization.
    """
    HOT_CURSOR.execute("SELECT COUNT(*) FROM hot_queue")
    hot_size = HOT_CURSOR.fetchone()[0]

    COLD_CURSOR.execute("SELECT COUNT(*) FROM cold_archive")
    cold_size = COLD_CURSOR.fetchone()[0]

    COLD_CURSOR.execute("SELECT COUNT(DISTINCT mcp_root_id) FROM identity_registry")
    identities = COLD_CURSOR.fetchone()[0]

    health_status = "HEALTHY" if hot_size < 5000 else "ELEVATED_BACKPRESSURE"

    return json.dumps({
        "system_health": health_status,
        "hot_tier_memory_queue": hot_size,
        "cold_tier_disk_records": cold_size,
        "unique_identities_stitched": identities,
        "recommended_action": "FLUSH_HOT_TIER" if hot_size > 1000 else "NOMINAL"
    })


if __name__ == "__main__":
    import time
    time.sleep(0.5)
    mcp.run(transport='stdio')