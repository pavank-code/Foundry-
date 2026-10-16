"""Small local backend for the CSV-cleaning prototype.

Keeps NVIDIA credentials server-side. Configure NVIDIA_API_KEY and NIM_MODEL
in the process environment before starting this file.
"""

from __future__ import annotations

import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
PAGE = ROOT / "csv_cleaning_prototype.html"
DEFAULT_ENDPOINT = "https://integrate.api.nvidia.com/v1/chat/completions"
ALLOWED_OPS = {"trim_whitespace", "remove_exact_duplicates", "require_non_null", "parse_date", "parse_number"}


def validate_plan(content: str, allowed_columns: list[str]) -> dict:
    """Parse JSON and enforce a small allowlist. Never execute model text."""
    candidate = content.strip()
    candidate = re.sub(r"^```(?:json)?\s*|\s*```$", "", candidate, flags=re.IGNORECASE)
    try:
        plan = json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise ValueError("NIM did not return valid JSON. Try a more specific prompt.") from exc
    if not isinstance(plan, dict) or not isinstance(plan.get("operations"), list):
        raise ValueError("NIM response must contain an operations array.")
    for operation in plan["operations"]:
        if not isinstance(operation, dict) or operation.get("type") not in ALLOWED_OPS:
            raise ValueError("NIM proposed an unsupported operation.")
        columns = operation.get("columns", [])
        if "column" in operation:
            columns = [*columns, operation["column"]]
        if not isinstance(columns, list) or any(col not in allowed_columns for col in columns):
            raise ValueError("NIM referred to a column that is not in the uploaded CSV.")
        if operation["type"] == "parse_date" and operation.get("format") not in {"yyyy-MM-dd", "MM/dd/yyyy", "dd/MM/yyyy"}:
            raise ValueError("NIM proposed an unsupported date format.")
    return plan


def call_nim(prompt: str, columns: list[str], profile: dict) -> dict:
    api_key = os.environ.get("NVIDIA_API_KEY", "").strip()
    model = os.environ.get("NIM_MODEL", "").strip()
    endpoint = os.environ.get("NIM_ENDPOINT", DEFAULT_ENDPOINT).strip()
    if not api_key:
        raise RuntimeError("NVIDIA_API_KEY is not set in the backend environment.")
    if not model:
        raise RuntimeError("NIM_MODEL is not set. Choose a model ID from your NVIDIA API Catalog model page.")
    if not endpoint.startswith("https://"):
        raise RuntimeError("NIM_ENDPOINT must use HTTPS for this hosted API configuration.")

    system = (
        "Convert the user's CSV-cleaning request into a JSON plan. Return JSON only, "
        "with this shape: {\"operations\":[{\"type\":\"...\",\"columns\":[...]}]}. "
        "Allowed type values: trim_whitespace, remove_exact_duplicates, require_non_null, parse_date, parse_number. "
        "For require_non_null and parse_date/parse_number use a single column field. "
        "parse_date must include format, selected only from yyyy-MM-dd, MM/dd/yyyy, dd/MM/yyyy. "
        "Use only column names supplied by the application. Do not return code, SQL, or extra operations. "
        "If the request is ambiguous, return {\"operations\":[],\"needs_clarification\":\"short question\"}."
    )
    user = {"request": prompt, "columns": columns, "profile": profile}
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        "temperature": 0,
        "max_tokens": 700,
        "stream": False,
    }
    request = Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=45) as response:
            data = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        # Never include request headers or the secret in an error returned to the browser.
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"NIM request failed with HTTP {exc.code}: {detail}") from None
    except (URLError, TimeoutError) as exc:
        raise RuntimeError(f"Could not reach the configured NIM endpoint: {exc}") from None

    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        raise RuntimeError("NIM response did not contain a chat completion.") from None
    return validate_plan(content, columns)


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, value: dict) -> None:
        self._send(status, json.dumps(value).encode("utf-8"), "application/json; charset=utf-8")

    def do_GET(self) -> None:
        if self.path == "/":
            try:
                self._send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
            except OSError:
                self._json(500, {"error": "Prototype HTML file was not found beside the backend."})
        elif self.path == "/api/status":
            self._json(200, {
                "api_key_configured": bool(os.environ.get("NVIDIA_API_KEY", "").strip()),
                "model_configured": bool(os.environ.get("NIM_MODEL", "").strip()),
                "endpoint": os.environ.get("NIM_ENDPOINT", DEFAULT_ENDPOINT),
                "note": "Configuration is present; this status does not prove a successful NIM call.",
            })
        else:
            self._json(404, {"error": "Not found"})

    def do_POST(self) -> None:
        if self.path != "/api/plan":
            self._json(404, {"error": "Not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 32_000:
                raise ValueError("Request body must be between 1 byte and 32 KB.")
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            prompt = body.get("prompt")
            columns = body.get("columns")
            profile = body.get("profile", {})
            if not isinstance(prompt, str) or not prompt.strip():
                raise ValueError("Cleaning prompt is required.")
            if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
                raise ValueError("CSV column names are required.")
            plan = call_nim(prompt[:4000], columns[:200], profile if isinstance(profile, dict) else {})
            self._json(200, {"provider": "NVIDIA NIM", "model": os.environ.get("NIM_MODEL"), "plan": plan})
        except (ValueError, json.JSONDecodeError) as exc:
            self._json(400, {"error": str(exc)})
        except RuntimeError as exc:
            self._json(502, {"error": str(exc)})

    def log_message(self, fmt: str, *args) -> None:
        # Do not log request bodies, prompts, or authorization material.
        print(f"{self.log_date_time_string()} {self.address_string()} {fmt % args}")


if __name__ == "__main__":
    host = os.environ.get("APP_HOST", "127.0.0.1")
    port = int(os.environ.get("APP_PORT", "8765"))
    print(f"Prototype server: http://{host}:{port}")
    print("NVIDIA_API_KEY value is never printed or sent to the browser.")
    ThreadingHTTPServer((host, port), Handler).serve_forever()
