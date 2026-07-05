import json
import time
from typing import Any, Dict, List

from sqlalchemy import text

from modules.state_log_db import get_state_log_engine


def _ensure_table() -> None:
    engine = get_state_log_engine()
    create_sql = (
        "CREATE TABLE IF NOT EXISTS telemetry_snapshots ("
        "id BIGSERIAL PRIMARY KEY, "
        "ts_ms BIGINT NOT NULL, "
        "source VARCHAR(64) NOT NULL DEFAULT '', "
        "note VARCHAR(255) NOT NULL DEFAULT '', "
        "scaler_json TEXT NOT NULL, "
        "hv_json TEXT NOT NULL"
        ")"
    )
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def save_snapshot(scaler_data: Dict[str, Any], hv_data: Dict[str, Any], source: str = "manual", note: str = "") -> Dict[str, Any]:
    _ensure_table()
    ts_ms = int(time.time() * 1000)
    payload = {
        "ts_ms": ts_ms,
        "source": str(source or ""),
        "note": str(note or ""),
        "scaler_json": json.dumps(scaler_data, ensure_ascii=False),
        "hv_json": json.dumps(hv_data, ensure_ascii=False),
    }
    engine = get_state_log_engine()
    insert_sql = text(
        "INSERT INTO telemetry_snapshots (ts_ms, source, note, scaler_json, hv_json) "
        "VALUES (:ts_ms, :source, :note, :scaler_json, :hv_json)"
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, payload)
    return {"message": "ok", "ts_ms": ts_ms}


def list_snapshots(limit: int = 100) -> List[Dict[str, Any]]:
    _ensure_table()
    engine = get_state_log_engine()
    query_sql = text(
        "SELECT id, ts_ms, source, note, scaler_json, hv_json "
        "FROM telemetry_snapshots ORDER BY ts_ms DESC LIMIT :limit"
    )
    rows: List[Dict[str, Any]] = []
    with engine.connect() as conn:
        result = conn.execute(query_sql, {"limit": int(limit)}).mappings().all()
        for row in result:
            rows.append(
                {
                    "id": row["id"],
                    "ts_ms": row["ts_ms"],
                    "source": row["source"],
                    "note": row["note"],
                    "scaler": json.loads(row["scaler_json"]),
                    "hv": json.loads(row["hv_json"]),
                }
            )
    return rows