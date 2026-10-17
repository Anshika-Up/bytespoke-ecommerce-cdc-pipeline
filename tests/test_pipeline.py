import json
import tempfile
import unittest
from pathlib import Path
from src.pipeline import connect, init_db, process_events, ValidationError
from src.ai_summary import RuleBasedSummarizer

def event(event_id, entity, operation, entity_id, data, time="2026-10-17T12:00:00Z"):
    return {"event_id":event_id,"entity":entity,"operation":operation,"entity_id":entity_id,
            "event_time":time,"data":data}

class BrokenSummarizer:
    name = "broken-test-provider"
    def summarize(self, changes):
        raise RuntimeError("simulated model outage")

class CDCPipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.conn = connect(Path(self.temp.name) / "test.db")
        init_db(self.conn)
        self.summarizer = RuleBasedSummarizer()

    def tearDown(self):
        self.conn.close()
        self.temp.cleanup()

    def test_insert_and_update_preserve_current_state(self):
        events = [
            event("e1","customer","INSERT","C1",{"name":"A","email":"a@example.test","city":"Gorakhpur"}),
            event("e2","customer","UPDATE","C1",{"city":"Lucknow"}),
            event("e3","order","INSERT","O1",{"customer_id":"C1","amount":100,"status":"PLACED"}),
            event("e4","order","UPDATE","O1",{"status":"SHIPPED"})
        ]
        result = process_events(self.conn, events, self.summarizer)
        customer = self.conn.execute("SELECT city FROM customers WHERE customer_id='C1'").fetchone()[0]
        status = self.conn.execute("SELECT status FROM orders WHERE order_id='O1'").fetchone()[0]
        self.assertEqual(result["processed"], 4)
        self.assertEqual(customer, "Lucknow")
        self.assertEqual(status, "SHIPPED")

    def test_duplicate_event_id_is_idempotent(self):
        e = event("e1","customer","INSERT","C1",{"name":"A","email":"a@example.test","city":"Gorakhpur"})
        first = process_events(self.conn, [e], self.summarizer)
        second = process_events(self.conn, [e], self.summarizer)
        count = self.conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        self.assertEqual(first["processed"], 1)
        self.assertEqual(second["processed"], 0)
        self.assertEqual(second["skipped"], 1)
        self.assertEqual(count, 1)

    def test_edge_case_rejects_update_for_unknown_customer(self):
        e = event("e1","customer","UPDATE","missing",{"city":"Lucknow"})
        with self.assertRaises(ValidationError):
            process_events(self.conn, [e], self.summarizer)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0], 0)

    def test_ai_provider_failure_uses_safe_fallback(self):
        e = event("e1","customer","INSERT","C1",{"name":"A","email":"a@example.test","city":"Gorakhpur"})
        result = process_events(self.conn, [e], BrokenSummarizer())
        self.assertIn("AI provider unavailable", result["provider"])
        self.assertIn("Processed 1 new change event", result["summary"])

    def test_edge_case_rejects_negative_order_amount(self):
        customer = event("e1","customer","INSERT","C1",{"name":"A","email":"a@example.test","city":"Gorakhpur"})
        process_events(self.conn, [customer], self.summarizer)
        order = event("e2","order","INSERT","O1",{"customer_id":"C1","amount":-5,"status":"PLACED"})
        with self.assertRaises(ValidationError):
            process_events(self.conn, [order], self.summarizer)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)

if __name__ == "__main__":
    unittest.main()
