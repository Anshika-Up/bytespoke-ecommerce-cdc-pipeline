"""AI summary interface with optional local Ollama integration and safe fallback."""
from __future__ import annotations
import json
import os
import urllib.request
import urllib.error

class SummaryProvider:
    """Small interface so model providers can be swapped without changing the pipeline."""
    name = "base"
    def summarize(self, changes: list[dict]) -> str:
        raise NotImplementedError

class RuleBasedSummarizer(SummaryProvider):
    """Deterministic offline fallback; never invents metrics."""
    name = "rule-based-fallback"
    def summarize(self, changes: list[dict]) -> str:
        if not changes:
            return "No new changes were processed."
        inserts = sum(1 for c in changes if c["operation"] == "INSERT")
        updates = sum(1 for c in changes if c["operation"] == "UPDATE")
        customers = sum(1 for c in changes if c["entity"] == "customer")
        orders = sum(1 for c in changes if c["entity"] == "order")
        examples = []
        for c in changes[:4]:
            examples.append(f'{c["entity"]} {c["entity_id"]}: {c["operation"]}')
        return (
            f"Processed {len(changes)} new change event(s): {inserts} insert(s), "
            f"{updates} update(s). Entity breakdown: {customers} customer event(s), "
            f"{orders} order event(s). Evidence: " + "; ".join(examples) + "."
        )

class OllamaSummarizer(SummaryProvider):
    """Optional local model adapter. Requires Ollama running locally; no cloud API key."""
    name = "ollama-local"
    def __init__(self, model: str | None = None, base_url: str | None = None):
        self.model = model or os.getenv("OLLAMA_MODEL", "llama3.2")
        self.base_url = (base_url or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")).rstrip("/")

    def summarize(self, changes: list[dict]) -> str:
        prompt = (
            "Summarize this e-commerce CDC batch in 2-4 sentences. Use only facts in the JSON. "
            "Do not invent counts or causes. Mention important state changes. JSON: "
            + json.dumps(changes, ensure_ascii=False)
        )
        body = json.dumps({"model": self.model, "prompt": prompt, "stream": False}).encode()
        req = urllib.request.Request(
            self.base_url + "/api/generate", data=body,
            headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode())
        text = result.get("response", "").strip()
        if not text:
            raise ValueError("Ollama returned an empty summary")
        return text

def get_summarizer() -> SummaryProvider:
    """Use local model only when explicitly enabled; otherwise use offline fallback."""
    if os.getenv("USE_OLLAMA", "").lower() in {"1", "true", "yes"}:
        return OllamaSummarizer()
    return RuleBasedSummarizer()
