# Smart City Spark Data Prep

## What the backend does

- Uses the supplied `locations.csv`, `public_complaints.csv`, and `incidents.csv` as its default inputs.
- Accepts optional replacement CSVs with those same filenames and a natural-language instruction.
- Sends the instruction and dataset names to NVIDIA NIM from the backend. The API key is read from `NVIDIA_API_KEY`; it is never placed in frontend code, request payloads, or output files.
- Validates the NIM response against three allowed operations. Spark performs schema checks, whitespace/category cleanup, duplicate and invalid-row handling, reference checks, joins, aggregations, and CSV output.
- Calculates an explainable priority heuristic in Spark. NIM does not set the weights or rankings.
- Exposes a status endpoint for the browser to poll, a 50-row preview, a quality report, a rejected-row file, and a downloadable ZIP.

The bundled `smart_city_dashboard.html` is only a reference UI. Pavan's frontend can integrate directly with the backend endpoints documented in `BACKEND_API_FOR_PAVAN.md`.

## Start

Install Python 3, Java, and the packages in `requirements_smart_city.txt` in the Python environment you will use to run the app. Spark requires a working Java runtime. Then run `run_smart_city.ps1`; it checks Python and package availability before asking for the API key. The key prompt is masked and the key is held only in that server process environment. Do not place the key in this folder or in the HTML.

The API root is `http://127.0.0.1:8765`; the bundled reference page is at `/` and interactive API docs are at `/docs`. Open the reference page through `http://127.0.0.1:8765/`, not by double-clicking the HTML file. If another frontend is running separately, it must call the full backend URL and its exact origin must be included in `FRONTEND_ORIGINS`; see `BACKEND_API_FOR_PAVAN.md`. The default source directory is `C:\Users\haard\Downloads\03_smart_city_where_fix_first`; set `SMART_CITY_DATA_DIR` before starting if you moved the files.

The backend uses the model ID supplied in the request: `nvidia/nemotron-3.5-super-vl-120b-a12b`, with `https://integrate.api.nvidia.com/v1` as the OpenAI-compatible base URL. It uses non-streaming output so the JSON plan can be validated before Spark receives it.

## Output files

- `priority_locations.csv`: one row per valid location, ranked by deterministic `priority_score`, with input metrics and reasons.
- `rejected_rows.csv`: invalid, duplicate-key, or unknown-location rows with a rejection reason and source record.
- `quality_report.json`: input, clean, and rejected counts plus the scoring formula.
- `nim_plan.json`: the validated NIM plan used for the run, or the deterministic fallback plan if NIM is unavailable.

Outputs are saved under `smart_city_results/<job_id>/` and packaged in the browser download.

## Score formula

- Road condition: Poor 20, Fair 10, Good/other 0.
- Near school: 10 when `near_school = 1`.
- Near hospital: 3 when `near_hospital = 1`.
- Pedestrian activity: High 8, Medium 4, Low/other 0.
- Open/assigned/in-progress complaints: 3 each, capped at 15.
- High-priority complaints: 2 each, capped at 10.
- Incidents: Critical 8, High 5, Medium 2, Low/other 1 each, capped at 35.
- Bands: Critical at 70+, High at 50+, Medium at 25+, otherwise Low.

This is a synthetic DATATHON heuristic, not an official city score or a validated public-safety decision tool.

## Important status note

This workspace did not have a runnable Python, Java, or Spark command available when the app was assembled. The files are wired for real Spark and NIM execution, but a live run still requires the dependencies and a valid local API key. If NIM is not configured or the call fails, the job records that fact and continues with the deterministic baseline Spark plan; it does not claim NIM succeeded.
