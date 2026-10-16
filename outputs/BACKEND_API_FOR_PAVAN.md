# Frontend integration contract for Pavan

This backend owns file validation, NVIDIA NIM plan generation, Apache Spark cleaning and joins, deterministic prioritization, status tracking, and output downloads. The frontend should call this API; it should **not** call NVIDIA NIM directly and must never receive `NVIDIA_API_KEY`.

## Local development URL

`http://127.0.0.1:8765`

OpenAPI documentation is available at `/docs`. For a separate Vite/React development server, allowed origins default to `localhost:3000` and `localhost:5173` (both localhost and 127.0.0.1 variants). Set `FRONTEND_ORIGINS` on the backend to change that list.

### Fix for browser “Failed to fetch”

The page must call the backend's full local API URL. A relative request such as `fetch('/api/status')` goes to the frontend's own server (for example, port 5173), not this backend. In a Vite frontend, set `VITE_API_BASE_URL=http://127.0.0.1:8765` in its `.env.local`, then use:

```js
const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8765";
const response = await fetch(`${API_BASE}/api/status`);
```

Use the same `API_BASE` for `/api/datasets`, `/api/jobs`, job polling, and downloads. If Pavan serves the frontend from a different origin/port, set the backend's `FRONTEND_ORIGINS` to the frontend's exact origin, for example `http://localhost:4173`, and restart the backend. Do not open the bundled dashboard as a `file://` page; start the backend and open `http://127.0.0.1:8765/` instead. Keep the backend terminal running while using the frontend.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/status` | Reports whether the backend sees a configured NIM key, whether PySpark is importable, and whether the three default CSVs are present. It never returns the key. |
| `GET` | `/api/datasets` | Lists the required input CSV filenames and whether each default source file exists. |
| `POST` | `/api/jobs` | Creates an asynchronous cleaning and ranking job. |
| `GET` | `/api/jobs/{job_id}` | Polls job status, progress, quality counts, and a preview when complete. |
| `GET` | `/api/jobs/{job_id}/download` | Downloads a ZIP containing the complete ranked CSV, rejected rows, quality report, and NIM plan. |

## Create a job

`POST /api/jobs` accepts JSON. `prompt` is required. `incoming_files` is optional; each uploaded item replaces the matching default file for that job. File names must be exactly `locations.csv`, `public_complaints.csv`, or `incidents.csv`. The backend otherwise uses the existing files in the configured data directory.

```json
{
  "prompt": "Trim whitespace, remove exact duplicates, and prioritize locations with poor roads, open complaints, serious incidents, school proximity, and high pedestrian activity. Keep the fixed score formula.",
  "incoming_files": [
    {
      "name": "locations.csv",
      "content": "location_id,zone,road_type,road_condition,population_density_per_km2,near_school,near_hospital,pedestrian_activity,latitude,longitude\nL0001,ZONE_19,Residential,Good,8597,0,0,Medium,16.516403,80.46389"
    }
  ]
}
```

The frontend should read selected files as text and send them as `content`. The current demo limit is 12 MB combined. The CSV contents are passed to Spark; only the user's prompt and the fixed file list are sent to NIM. No row contents are sent to NIM.

Successful response:

```json
{
  "job_id": "a1b2c3d4e5f6",
  "status_url": "/api/jobs/a1b2c3d4e5f6"
}
```

`download_url` is relative. The frontend should prefix it with `API_BASE`, for example `window.open(`${API_BASE}${job.download_url}`)`.

## Poll job status

Poll `GET /api/jobs/{job_id}` about once per second. Continue while `status` is `queued`, `preparing`, `nim_planning`, or `spark_cleaning`. Stop on `completed` or `failed`.

Completed response shape:

```json
{
  "job_id": "a1b2c3d4e5f6",
  "status": "completed",
  "progress": 100,
  "message": "Spark pipeline complete.",
  "nim_warning": null,
  "report": {
    "input_rows": {},
    "clean_rows": {},
    "rejected_rows": {},
    "score_method": {}
  },
  "preview": [],
  "download_url": "/api/jobs/a1b2c3d4e5f6/download"
}
```

`preview` contains up to 50 top-ranked location records. Its output columns are:

```text
location_id, zone, latitude, longitude, road_type, road_condition,
population_density_per_km2, near_school, near_hospital, pedestrian_activity,
complaint_count, open_complaint_count, high_priority_complaints,
incident_count, unresolved_incident_count, critical_incidents,
high_severity_incidents, medium_severity_incidents, reported_injuries,
priority_score, priority_band, priority_reasons
```

Failed response has `status: "failed"` and a human-readable `error`. A missing or failed NIM call is reported in `nim_warning`; Spark continues with its deterministic baseline cleaning plan. A missing Spark installation fails the job rather than returning simulated output.

## Responsibility boundary

- NIM converts the prompt into a constrained preprocessing plan. The backend validates its operation and dataset names before Spark uses the plan.
- Spark performs the actual cleaning, type validation, duplicate/reference checks, joins, counts, and output writing.
- Spark calculates `priority_score` using the fixed weights documented in `SMART_CITY_APP_README.md`. NIM cannot set weights or directly create the rankings.
- The dataset is synthetic and educational; scores are a heuristic, not a real-city safety recommendation.

## Secret handling

The backend reads `NVIDIA_API_KEY` from its process environment and uses it as the NIM bearer credential. No frontend form, API payload, response, or downloadable result contains the key. Pavan should display only the safe backend status boolean, e.g. “NIM connected” or “NIM unavailable”; never request or store the secret in browser storage.
