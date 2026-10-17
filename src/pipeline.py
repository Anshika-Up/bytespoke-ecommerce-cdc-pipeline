"""Incremental e-commerce CDC pipeline backed by SQLite."""
from __future__ import annotations
import argparse
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterable
from src.ai_summary import get_summarizer, SummaryProvider

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "cdc_pipeline.db"
SCHEMA_PATH = ROOT / "sql" / "schema.sql"
DEFAULT_INPUT = ROOT / "data" / "changes.json"

class ValidationError(ValueError):
    pass

def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def connect(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

def validate_event(event: dict) -> None:
    required = {"event_id", "entity", "operation", "entity_id", "event_time", "data"}
    missing = required - event.keys()
    if missing:
        raise ValidationError(f"Missing required field(s): {', '.join(sorted(missing))}")
    if not all(isinstance(event[k], str) and event[k].strip() for k in
               ("event_id", "entity", "operation", "entity_id", "event_time")):
        raise ValidationError("event_id, entity, operation, entity_id and event_time must be non-empty strings")
    if event["entity"] not in {"customer", "order"}:
        raise ValidationError(f"Unsupported entity: {event['entity']}")
    if event["operation"] not in {"INSERT", "UPDATE"}:
        raise ValidationError(f"Unsupported operation: {event['operation']}")
    if not isinstance(event["data"], dict) or not event["data"]:
        raise ValidationError("data must be a non-empty object")
    try:
        datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError("event_time must be ISO-8601") from exc
    if event["entity"] == "order" and "amount" in event["data"]:
        try:
            amount = float(event["data"]["amount"])
        except (TypeError, ValueError) as exc:
            raise ValidationError("order amount must be numeric") from exc
        if amount < 0:
            raise ValidationError("order amount cannot be negative")

def _upsert_customer(conn: sqlite3.Connection, event: dict) -> None:
    eid, op, data, t = event["entity_id"], event["operation"], event["data"], event["event_time"]
    existing = conn.execute("SELECT * FROM customers WHERE customer_id=?", (eid,)).fetchone()
    if existing is None and op == "UPDATE":
        raise ValidationError(f"Cannot UPDATE missing customer {eid}; send INSERT first")
    if existing is None:
        if not data.get("name") or not data.get("email") or not data.get("city"):
            raise ValidationError("Customer INSERT requires name, email and city")
        conn.execute("""INSERT INTO customers(customer_id,name,email,city,last_event_time)
                        VALUES(?,?,?,?,?)""", (eid, data["name"], data["email"], data["city"], t))
    else:
        merged = {k: existing[k] for k in ("name", "email", "city")}
        merged.update({k: v for k, v in data.items() if k in merged})
        conn.execute("""UPDATE customers SET name=?,email=?,city=?,last_event_time=?
                        WHERE customer_id=?""",
                     (merged["name"], merged["email"], merged["city"], t, eid))

def _upsert_order(conn: sqlite3.Connection, event: dict) -> None:
    eid, op, data, t = event["entity_id"], event["operation"], event["data"], event["event_time"]
    existing = conn.execute("SELECT * FROM orders WHERE order_id=?", (eid,)).fetchone()
    if existing is None and op == "UPDATE":
        raise ValidationError(f"Cannot UPDATE missing order {eid}; send INSERT first")
    if existing is None:
        required = {"customer_id", "amount", "status"}
        if not required.issubset(data):
            raise ValidationError("Order INSERT requires customer_id, amount and status")
        customer = conn.execute("SELECT 1 FROM customers WHERE customer_id=?", (data["customer_id"],)).fetchone()
        if not customer:
            raise ValidationError(f"Unknown customer_id {data['customer_id']}; insert customer first")
        amount = float(data["amount"])
        if amount < 0:
            raise ValidationError("order amount cannot be negative")
        conn.execute("""INSERT INTO orders(order_id,customer_id,amount,status,last_event_time)
                        VALUES(?,?,?,?,?)""",
                     (eid, data["customer_id"], amount, data["status"], t))
    else:
        merged = {k: existing[k] for k in ("customer_id", "amount", "status")}
        merged.update({k: v for k, v in data.items() if k in merged})
        if float(merged["amount"]) < 0:
            raise ValidationError("order amount cannot be negative")
        customer = conn.execute("SELECT 1 FROM customers WHERE customer_id=?", (merged["customer_id"],)).fetchone()
        if not customer:
            raise ValidationError(f"Unknown customer_id {merged['customer_id']}")
        conn.execute("""UPDATE orders SET customer_id=?,amount=?,status=?,last_event_time=?
                        WHERE order_id=?""",
                     (merged["customer_id"], float(merged["amount"]), merged["status"], t, eid))

def process_events(conn: sqlite3.Connection, events: Iterable[dict],
                   summarizer: SummaryProvider | None = None) -> dict:
    """Apply unseen events atomically. Duplicate event IDs are skipped (idempotency)."""
    init_db(conn)
    processed, skipped, change_evidence = 0, 0, []
    for event in events:
        validate_event(event)
        try:
            conn.execute("BEGIN")
            exists = conn.execute("SELECT 1 FROM raw_events WHERE event_id=?", (event["event_id"],)).fetchone()
            if exists:
                conn.rollback()
                skipped += 1
                continue
            # Apply state change first; if invalid, neither raw nor current-state data is committed.
            if event["entity"] == "customer":
                _upsert_customer(conn, event)
            else:
                _upsert_order(conn, event)
            conn.execute("""INSERT INTO raw_events(event_id,entity,operation,entity_id,event_time,payload_json)
                            VALUES(?,?,?,?,?,?)""",
                         (event["event_id"], event["entity"], event["operation"], event["entity_id"],
                          event["event_time"], json.dumps(event["data"], sort_keys=True)))
            conn.commit()
            processed += 1
            change_evidence.append({k: event[k] for k in ("event_id","entity","operation","entity_id","event_time","data")})
        except Exception:
            conn.rollback()
            raise
    summary_text = (summarizer or get_summarizer()).summarize(change_evidence)
    provider = (summarizer or get_summarizer()).name
    conn.execute("""INSERT INTO change_summaries(event_count,summary_text,provider) VALUES(?,?,?)""",
                 (processed, summary_text, provider))
    conn.commit()
    return {"processed": processed, "skipped": skipped, "summary": summary_text, "provider": provider}

def load_events(path: Path | str = DEFAULT_INPUT) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValidationError("Input JSON must be a list of events")
    return data

def show_state(conn: sqlite3.Connection) -> None:
    print("\nCURRENT CUSTOMERS")
    for r in conn.execute("SELECT * FROM customers ORDER BY customer_id"):
        print(dict(r))
    print("\nCURRENT ORDERS")
    for r in conn.execute("SELECT * FROM orders ORDER BY order_id"):
        print(dict(r))

def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental e-commerce CDC pipeline")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="JSON file containing CDC events")
    parser.add_argument("--db", default=str(DB_PATH), help="SQLite database path")
    parser.add_argument("--show-state", action="store_true", help="Print current-state tables")
    args = parser.parse_args()
    conn = connect(args.db)
    result = process_events(conn, load_events(args.input))
    print(json.dumps(result, indent=2))
    if args.show_state:
        show_state(conn)
    conn.close()

if __name__ == "__main__":
    main()
