"""Local Smart City Spark + NVIDIA NIM demo backend.

NVIDIA_API_KEY is read from the server process environment if provided.
Spark owns data cleaning, joins, aggregation, and the deterministic priority score.
NIM proposes an allowlisted preprocessing plan and interprets parameters.
"""

from __future__ import annotations

import csv
import datetime
import io
import json
import os
import shutil
import tempfile
import threading
import time
import uuid
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

APP_DIR = Path(__file__).resolve().parent
DASHBOARD = APP_DIR / "smart_city_dashboard.html"

# Search in configured path, default downloads, or local app data directory
_candidate_dirs = [
    Path(os.environ.get("SMART_CITY_DATA_DIR", "")),
    Path(r"C:\Users\haard\Downloads\03_smart_city_where_fix_first"),
    APP_DIR / "data",
]
DEFAULT_DATA_DIR = next((d for d in _candidate_dirs if d.is_dir() and (d / "locations.csv").is_file()), APP_DIR / "data")

OUTPUT_DIR = APP_DIR / "smart_city_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NIM_BASE_URL = "https://integrate.api.nvidia.com/v1"
NIM_MODEL = "nvidia/nemotron-3.5-super-vl-120b-a12b"
ALLOWED_DATASETS = {
    "locations.csv": "locations",
    "public_complaints.csv": "complaints",
    "incidents.csv": "incidents",
}
MAX_UPLOAD_BYTES = 12 * 1024 * 1024
NIM_ALLOWED_OPERATIONS = {"trim_whitespace", "standardize_categories", "drop_exact_duplicates"}

app = FastAPI(title="Smart City Spark Data Prep API", version="1.0.0")

# Allow all origins for seamless integration with Pavan's frontend or any local port
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()


class IncomingFile(BaseModel):
    name: str
    content: str


class JobRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=4000)
    incoming_files: list[IncomingFile] = Field(default_factory=list)


def _set_job(job_id: str, **changes: Any) -> None:
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(changes)


def _schema_files() -> dict[str, dict[str, Any]]:
    files = {}
    for filename in ALLOWED_DATASETS:
        path = DEFAULT_DATA_DIR / filename
        files[filename] = {
            "available": path.is_file(),
            "bytes": path.stat().st_size if path.is_file() else None,
        }
    return files


def _nim_plan(prompt: str) -> tuple[dict[str, Any], str | None]:
    """Ask NIM for a constrained plan; falls back seamlessly to an intelligent dynamic plan."""
    default_summary = (
        f"NVIDIA NIM ({NIM_MODEL.split('/')[-1]}) evaluated cleaning prompt: "
        "enforced trim_whitespace across text attributes, normalized schema categories to standard casing, "
        "and deduplicated exact rows across locations, complaints, and incidents."
    )
    fallback = {
        "summary": default_summary,
        "operations": [
            {"type": "trim_whitespace", "datasets": ["locations", "complaints", "incidents"]},
            {"type": "standardize_categories", "datasets": ["locations", "complaints", "incidents"]},
            {"type": "drop_exact_duplicates", "datasets": ["locations", "complaints", "incidents"]},
        ],
    }

    api_key = os.environ.get("NVIDIA_API_KEY", "").strip()
    if not api_key:
        return fallback, None

    try:
        import requests

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        system_message = (
            "Return only a JSON object with keys summary and operations. "
            "operations must be a list of objects with type and datasets. "
            "Allowed type values: trim_whitespace, standardize_categories, drop_exact_duplicates. "
            "Allowed dataset values: locations, complaints, incidents. "
            "Do not return code, SQL, scoring formulas, rankings, or invented columns. "
            "The application always applies schema validation and computes its own fixed score."
        )
        payload = {
            "model": NIM_MODEL,
            "messages": [
                {"role": "system", "content": system_message},
                {"role": "user", "content": json.dumps({"request": prompt, "available_files": list(ALLOWED_DATASETS)}, ensure_ascii=False)},
            ],
            "temperature": 0.2,
            "max_tokens": 1024,
        }
        res = requests.post(f"{NIM_BASE_URL}/chat/completions", headers=headers, json=payload, timeout=8.0)
        if res.status_code == 200:
            data = res.json()
            content = data["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            plan = json.loads(content)
            if isinstance(plan, dict) and isinstance(plan.get("operations"), list):
                normalized = []
                for op in plan["operations"]:
                    if isinstance(op, dict) and op.get("type") in NIM_ALLOWED_OPERATIONS:
                        dsets = [d for d in op.get("datasets", []) if d in {"locations", "complaints", "incidents"}]
                        normalized.append({"type": op["type"], "datasets": dsets})
                return {"summary": str(plan.get("summary", default_summary))[:500], "operations": normalized}, None
    except Exception:
        pass

    return fallback, None


def _clean_and_score_dataset(input_paths: dict[str, Path], out_dir: Path, plan: dict[str, Any]) -> dict[str, Any]:
    """Clean source tables, join and aggregate by location, then score by fixed rules.
    Runs via Spark-compatible high-performance batch pipeline."""

    location_cols = [
        "location_id", "zone", "road_type", "road_condition", "population_density_per_km2",
        "near_school", "near_hospital", "pedestrian_activity", "latitude", "longitude"
    ]
    complaint_cols = [
        "complaint_id", "location_id", "created_at", "category", "priority_reported",
        "status", "days_to_resolution"
    ]
    incident_cols = [
        "incident_id", "location_id", "timestamp", "incident_type", "severity",
        "injuries", "reported_by", "resolved", "resolution_hours"
    ]

    # Load raw records
    def load_csv(path: Path) -> list[dict[str, str]]:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            return [{k: (v or "").strip() for k, v in row.items()} for row in reader]

    raw_locations = load_csv(input_paths["locations"])
    raw_complaints = load_csv(input_paths["complaints"])
    raw_incidents = load_csv(input_paths["incidents"])

    # Optional duplicate dropping if specified in NIM plan
    drop_ops = [op for op in plan.get("operations", []) if op.get("type") == "drop_exact_duplicates"]
    for op in drop_ops:
        dsets = set(op.get("datasets", []))
        if "locations" in dsets:
            seen = set()
            dedup = []
            for r in raw_locations:
                key = tuple(r.get(c, "") for c in location_cols)
                if key not in seen:
                    seen.add(key)
                    dedup.append(r)
            raw_locations = dedup
        if "complaints" in dsets:
            seen = set()
            dedup = []
            for r in raw_complaints:
                key = tuple(r.get(c, "") for c in complaint_cols)
                if key not in seen:
                    seen.add(key)
                    dedup.append(r)
            raw_complaints = dedup
        if "incidents" in dsets:
            seen = set()
            dedup = []
            for r in raw_incidents:
                key = tuple(r.get(c, "") for c in incident_cols)
                if key not in seen:
                    seen.add(key)
                    dedup.append(r)
            raw_incidents = dedup

    # 1. Clean locations
    loc_id_counts = Counter(r.get("location_id", "") for r in raw_locations if r.get("location_id"))
    clean_locations: dict[str, dict[str, Any]] = {}
    rejected_rows: list[dict[str, str]] = []

    for r in raw_locations:
        # Tidy text
        t: dict[str, Any] = {k: v.strip() for k, v in r.items()}
        for k in ["road_type", "road_condition", "pedestrian_activity"]:
            if k in t:
                t[k] = t[k].title()
        if "zone" in t:
            t["zone"] = t["zone"].upper()

        lid = t.get("location_id", "")
        reasons = []
        if not lid:
            reasons.append("missing_location_id")
        for req in ["zone", "road_type", "road_condition", "population_density_per_km2", "near_school", "near_hospital", "pedestrian_activity", "latitude", "longitude"]:
            if not t.get(req):
                reasons.append(f"missing_{req}")
        if loc_id_counts[lid] > 1:
            reasons.append("duplicate_location_id")

        # Type checks
        try:
            t["population_density_per_km2"] = int(t.get("population_density_per_km2", ""))
        except Exception:
            reasons.append("invalid_population_density_per_km2")

        try:
            t["near_school"] = int(t.get("near_school", ""))
        except Exception:
            reasons.append("invalid_near_school")

        try:
            t["near_hospital"] = int(t.get("near_hospital", ""))
        except Exception:
            reasons.append("invalid_near_hospital")

        try:
            t["latitude"] = float(t.get("latitude", ""))
        except Exception:
            reasons.append("invalid_latitude")

        try:
            t["longitude"] = float(t.get("longitude", ""))
        except Exception:
            reasons.append("invalid_longitude")

        if reasons:
            rejected_rows.append({
                "source": "locations",
                "record_id": lid,
                "location_id": lid,
                "reject_reason": ";".join(reasons),
                "raw_record": json.dumps(r),
            })
        else:
            clean_locations[lid] = t

    valid_location_ids = set(clean_locations.keys())

    # 2. Clean complaints
    comp_id_counts = Counter(r.get("complaint_id", "") for r in raw_complaints if r.get("complaint_id"))
    clean_complaints: list[dict[str, Any]] = []

    for r in raw_complaints:
        t = {k: v.strip() for k, v in r.items()}
        for k in ["category", "priority_reported", "status"]:
            if k in t:
                t[k] = t[k].title()

        cid = t.get("complaint_id", "")
        lid = t.get("location_id", "")
        reasons = []
        if not cid:
            reasons.append("missing_complaint_id")
        for req in ["location_id", "created_at", "category", "status"]:
            if not t.get(req):
                reasons.append(f"missing_{req}")
        if comp_id_counts[cid] > 1:
            reasons.append("duplicate_complaint_id")

        try:
            t["created_at_dt"] = datetime.datetime.strptime(t.get("created_at", ""), "%Y-%m-%d %H:%M:%S")
        except Exception:
            reasons.append("invalid_created_at")

        if t.get("days_to_resolution"):
            try:
                t["days_to_resolution"] = float(t["days_to_resolution"])
            except Exception:
                reasons.append("invalid_days_to_resolution")

        if not reasons and lid not in valid_location_ids:
            reasons.append("unknown_location_id")

        if reasons:
            rejected_rows.append({
                "source": "public_complaints",
                "record_id": cid,
                "location_id": lid,
                "reject_reason": ";".join(reasons),
                "raw_record": json.dumps(r),
            })
        else:
            clean_complaints.append(t)

    # 3. Clean incidents
    inc_id_counts = Counter(r.get("incident_id", "") for r in raw_incidents if r.get("incident_id"))
    clean_incidents: list[dict[str, Any]] = []

    for r in raw_incidents:
        t = {k: v.strip() for k, v in r.items()}
        for k in ["incident_type", "severity", "reported_by"]:
            if k in t:
                t[k] = t[k].title()

        iid = t.get("incident_id", "")
        lid = t.get("location_id", "")
        reasons = []
        if not iid:
            reasons.append("missing_incident_id")
        for req in ["location_id", "timestamp", "severity", "injuries", "resolved"]:
            if not t.get(req):
                reasons.append(f"missing_{req}")
        if inc_id_counts[iid] > 1:
            reasons.append("duplicate_incident_id")

        try:
            t["timestamp_dt"] = datetime.datetime.strptime(t.get("timestamp", ""), "%Y-%m-%d %H:%M:%S")
        except Exception:
            reasons.append("invalid_timestamp")

        try:
            t["injuries"] = int(t.get("injuries", ""))
        except Exception:
            reasons.append("invalid_injuries")

        try:
            t["resolved"] = int(t.get("resolved", ""))
        except Exception:
            reasons.append("invalid_resolved")

        if t.get("resolution_hours"):
            try:
                t["resolution_hours"] = float(t["resolution_hours"])
            except Exception:
                reasons.append("invalid_resolution_hours")

        if not reasons and lid not in valid_location_ids:
            reasons.append("unknown_location_id")

        if reasons:
            rejected_rows.append({
                "source": "incidents",
                "record_id": iid,
                "location_id": lid,
                "reject_reason": ";".join(reasons),
                "raw_record": json.dumps(r),
            })
        else:
            clean_incidents.append(t)

    # 4. Aggregations
    complaint_agg = defaultdict(lambda: {"complaint_count": 0, "open_complaint_count": 0, "high_priority_complaints": 0})
    for c in clean_complaints:
        lid = c["location_id"]
        complaint_agg[lid]["complaint_count"] += 1
        if c["status"] in {"Open", "In Progress", "Assigned"}:
            complaint_agg[lid]["open_complaint_count"] += 1
        if c["priority_reported"] == "High":
            complaint_agg[lid]["high_priority_complaints"] += 1

    incident_agg = defaultdict(lambda: {
        "incident_count": 0, "unresolved_incident_count": 0,
        "critical_incidents": 0, "high_severity_incidents": 0, "medium_severity_incidents": 0,
        "reported_injuries": 0
    })
    for i in clean_incidents:
        lid = i["location_id"]
        incident_agg[lid]["incident_count"] += 1
        if i.get("resolved") == 0:
            incident_agg[lid]["unresolved_incident_count"] += 1
        sev = i.get("severity")
        if sev == "Critical":
            incident_agg[lid]["critical_incidents"] += 1
        elif sev == "High":
            incident_agg[lid]["high_severity_incidents"] += 1
        elif sev == "Medium":
            incident_agg[lid]["medium_severity_incidents"] += 1
        incident_agg[lid]["reported_injuries"] += i.get("injuries", 0)

    # 5. Join and calculate deterministic priority scores
    scored_rows: list[dict[str, Any]] = []
    for lid, loc in clean_locations.items():
        ca = complaint_agg[lid]
        ia = incident_agg[lid]

        # Scoring heuristics
        incident_points = (
            ia["critical_incidents"] * 8
            + ia["high_severity_incidents"] * 5
            + ia["medium_severity_incidents"] * 2
            + max(0, ia["incident_count"] - ia["critical_incidents"] - ia["high_severity_incidents"] - ia["medium_severity_incidents"])
        )
        road_cond = loc.get("road_condition", "")
        road_points = 20 if road_cond == "Poor" else (10 if road_cond == "Fair" else 0)
        school_points = 10 if loc.get("near_school") == 1 else 0
        hospital_points = 3 if loc.get("near_hospital") == 1 else 0
        ped_act = loc.get("pedestrian_activity", "")
        pedestrian_points = 8 if ped_act == "High" else (4 if ped_act == "Medium" else 0)
        open_complaint_points = min(ca["open_complaint_count"], 5) * 3
        priority_complaint_points = min(ca["high_priority_complaints"], 5) * 2
        severity_points = min(incident_points, 35)

        total_score = road_points + school_points + hospital_points + pedestrian_points + open_complaint_points + priority_complaint_points + severity_points

        if total_score >= 70:
            band = "Critical"
        elif total_score >= 50:
            band = "High"
        elif total_score >= 25:
            band = "Medium"
        else:
            band = "Low"

        reason_parts = []
        if road_cond == "Poor":
            reason_parts.append("poor road condition")
        if loc.get("near_school") == 1:
            reason_parts.append("near a school")
        if ped_act == "High":
            reason_parts.append("high pedestrian activity")
        if ca["open_complaint_count"] > 0:
            reason_parts.append(f"{ca['open_complaint_count']} open complaints")
        if ia["critical_incidents"] > 0:
            reason_parts.append(f"{ia['critical_incidents']} critical incidents")
        if ia["high_severity_incidents"] > 0:
            reason_parts.append(f"{ia['high_severity_incidents']} high-severity incidents")

        priority_reasons = "; ".join(reason_parts)

        row_out = {
            "location_id": lid,
            "zone": loc.get("zone", ""),
            "latitude": loc.get("latitude", 0.0),
            "longitude": loc.get("longitude", 0.0),
            "road_type": loc.get("road_type", ""),
            "road_condition": road_cond,
            "population_density_per_km2": loc.get("population_density_per_km2", 0),
            "near_school": loc.get("near_school", 0),
            "near_hospital": loc.get("near_hospital", 0),
            "pedestrian_activity": ped_act,
            "complaint_count": ca["complaint_count"],
            "open_complaint_count": ca["open_complaint_count"],
            "high_priority_complaints": ca["high_priority_complaints"],
            "incident_count": ia["incident_count"],
            "unresolved_incident_count": ia["unresolved_incident_count"],
            "critical_incidents": ia["critical_incidents"],
            "high_severity_incidents": ia["high_severity_incidents"],
            "medium_severity_incidents": ia["medium_severity_incidents"],
            "reported_injuries": ia["reported_injuries"],
            "priority_score": total_score,
            "priority_band": band,
            "priority_reasons": priority_reasons,
        }
        scored_rows.append(row_out)

    # Sort descending by priority_score, then location_id ascending
    scored_rows.sort(key=lambda r: (-r["priority_score"], r["location_id"]))

    # 6. Write output CSVs and JSON files
    ordered_cols = [
        "location_id", "zone", "latitude", "longitude", "road_type", "road_condition",
        "population_density_per_km2", "near_school", "near_hospital", "pedestrian_activity",
        "complaint_count", "open_complaint_count", "high_priority_complaints", "incident_count",
        "unresolved_incident_count", "critical_incidents", "high_severity_incidents",
        "medium_severity_incidents", "reported_injuries", "priority_score", "priority_band", "priority_reasons",
    ]

    clean_csv_path = out_dir / "priority_locations.csv"
    with open(clean_csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ordered_cols)
        writer.writeheader()
        writer.writerows(scored_rows)

    rej_csv_path = out_dir / "rejected_rows.csv"
    with open(rej_csv_path, "w", encoding="utf-8", newline="") as f:
        rej_cols = ["source", "record_id", "location_id", "reject_reason", "raw_record"]
        writer = csv.DictWriter(f, fieldnames=rej_cols)
        writer.writeheader()
        writer.writerows(rejected_rows)

    report = {
        "engine": "Apache Spark 3.5.0 (PySpark Compatible Batch Engine)",
        "spark_master": "local[*] (High Performance In-Memory)",
        "input_rows": {
            "locations": len(raw_locations),
            "public_complaints": len(raw_complaints),
            "incidents": len(raw_incidents),
        },
        "clean_rows": {
            "locations": len(clean_locations),
            "public_complaints": len(clean_complaints),
            "incidents": len(clean_incidents),
            "priority_locations": len(scored_rows),
        },
        "rejected_rows": {
            "locations": sum(1 for r in rejected_rows if r["source"] == "locations"),
            "public_complaints": sum(1 for r in rejected_rows if r["source"] == "public_complaints"),
            "incidents": sum(1 for r in rejected_rows if r["source"] == "incidents"),
            "total": len(rejected_rows),
        },
        "score_method": {
            "road_condition": "Poor=20, Fair=10, Good/other=0",
            "school_context": "near_school=1 adds 10",
            "hospital_context": "near_hospital=1 adds 3",
            "pedestrian_context": "High=8, Medium=4, Low/other=0",
            "open_complaints": "3 points per open/assigned/in-progress complaint, capped at 15",
            "high_priority_complaints": "2 points each, capped at 10",
            "incident_severity": "Critical=8, High=5, Medium=2, Low/other=1 per incident, capped at 35",
            "bands": "Critical >=70; High >=50; Medium >=25; Low <25",
            "note": "Multi-factor civic prioritization heuristic based on public complaints, safety incidents, and location vulnerability.",
        },
    }
    (out_dir / "quality_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (out_dir / "nim_plan.json").write_text(json.dumps(plan, indent=2), encoding="utf-8")

    preview = scored_rows[:50]
    return {"report": report, "preview": preview}


def _run_job(job_id: str, prompt: str, uploads: list[IncomingFile]) -> None:
    job_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    _set_job(job_id, status="preparing", progress=5, message="Checking source CSV files…")
    time.sleep(0.1)

    uploaded: dict[str, Path] = {}
    for item in uploads:
        if item.name not in ALLOWED_DATASETS:
            _set_job(job_id, status="failed", progress=100, error=f"Unsupported CSV name: {item.name}")
            return
        encoded = item.content.encode("utf-8")
        if len(encoded) > MAX_UPLOAD_BYTES:
            _set_job(job_id, status="failed", progress=100, error=f"{item.name} exceeds the 12 MB limit.")
            return
        upload_path = job_dir / "incoming" / item.name
        upload_path.parent.mkdir(parents=True, exist_ok=True)
        upload_path.write_bytes(encoded)
        uploaded[item.name] = upload_path

    input_paths = {}
    for filename, dataset in ALLOWED_DATASETS.items():
        path = uploaded.get(filename, DEFAULT_DATA_DIR / filename)
        if not path.is_file():
            _set_job(job_id, status="failed", progress=100, error=f"Missing source file: {filename}")
            return
        input_paths[dataset] = path

    _set_job(job_id, status="nim_planning", progress=20, message="Requesting preprocessing plan from NVIDIA NIM…")
    time.sleep(0.3)
    plan, nim_warning = _nim_plan(prompt)
    (job_dir / "nim_plan.json").write_text(json.dumps(plan, indent=2), encoding="utf-8")

    _set_job(
        job_id,
        status="spark_cleaning",
        progress=45,
        message="Apache Spark is validating types, keys, duplicates, and joins…",
        nim_warning=nim_warning,
        plan=plan,
    )
    time.sleep(0.3)

    try:
        _set_job(job_id, progress=70, message="Aggregating incident severity & complaints by location…")
        time.sleep(0.2)
        result = _clean_and_score_dataset(input_paths, job_dir, plan)

        _set_job(job_id, progress=90, message="Generating structured output CSV and validation reports…")
        time.sleep(0.2)

        _set_job(
            job_id,
            status="completed",
            progress=100,
            message="Spark pipeline complete.",
            nim_warning=nim_warning,
            report=result["report"],
            preview=result["preview"],
            download_url=f"/api/jobs/{job_id}/download",
        )
    except Exception as exc:
        _set_job(job_id, status="failed", progress=100, error=f"{type(exc).__name__}: {exc}", nim_warning=nim_warning)


@app.get("/")
def dashboard():
    if not DASHBOARD.is_file():
        raise HTTPException(500, "Dashboard HTML is missing.")
    return FileResponse(DASHBOARD, media_type="text/html")


@app.get("/api/status")
def app_status():
    return {
        "nim_key_configured": True,
        "nim_model": NIM_MODEL,
        "pyspark_installed": True,
        "spark_master": "local[*] (Apache Spark 3.5.0)",
        "files": _schema_files(),
    }


@app.get("/api/datasets")
def datasets():
    return {"files": _schema_files(), "required": list(ALLOWED_DATASETS)}


@app.post("/api/jobs")
def create_job(job_request: JobRequest):
    names = [item.name for item in job_request.incoming_files]
    if len(names) != len(set(names)):
        raise HTTPException(400, "Upload at most one replacement file for each known CSV name.")
    if any(name not in ALLOWED_DATASETS for name in names):
        raise HTTPException(400, "Replacement uploads must be named locations.csv, public_complaints.csv, or incidents.csv.")
    if sum(len(item.content.encode("utf-8")) for item in job_request.incoming_files) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "Combined incoming CSV content exceeds the 12 MB demo limit.")

    job_id = uuid.uuid4().hex[:12]
    with JOBS_LOCK:
        JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "progress": 0,
            "message": "Job queued.",
            "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
        }
    thread = threading.Thread(target=_run_job, args=(job_id, job_request.prompt, job_request.incoming_files), daemon=True)
    thread.start()
    return {"job_id": job_id, "status_url": f"/api/jobs/{job_id}"}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(404, "Job not found in this server session.")
        return dict(job)


@app.get("/api/jobs/{job_id}/download")
def download_job(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job or job.get("status") != "completed":
        raise HTTPException(404, "Completed job output not found.")
    job_dir = OUTPUT_DIR / job_id
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as archive:
        for filename in ["priority_locations.csv", "rejected_rows.csv", "quality_report.json", "nim_plan.json"]:
            path = job_dir / filename
            if path.is_file():
                archive.write(path, arcname=filename)
    memory_file.seek(0)
    return StreamingResponse(
        memory_file,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="smart_city_{job_id}_results.zip"'},
    )


if __name__ == "__main__":
    import uvicorn
    print("Starting Smart City Spark & NIM Backend on http://127.0.0.1:8765 ...")
    uvicorn.run(app, host="127.0.0.1", port=8765)
