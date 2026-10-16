"""Ontology-lite DB schema for Where Should We Fix First."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "city.db"

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS data_sources (
  id INTEGER PRIMARY KEY, name TEXT UNIQUE, kind TEXT, path TEXT, created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS ingestion_batches (
  id INTEGER PRIMARY KEY, source_id INTEGER, started_at TEXT DEFAULT (datetime('now')),
  status TEXT, rows_total INT, rows_accepted INT, rows_quarantined INT, notes TEXT
);
CREATE TABLE IF NOT EXISTS raw_records (
  id INTEGER PRIMARY KEY, batch_id INTEGER, source_table TEXT, source_key TEXT, payload TEXT, content_hash TEXT
);
CREATE TABLE IF NOT EXISTS validation_issues (
  id INTEGER PRIMARY KEY, batch_id INTEGER, source_table TEXT, source_key TEXT,
  rule_code TEXT, field TEXT, reason TEXT, status TEXT DEFAULT 'open'
);
CREATE TABLE IF NOT EXISTS locations (
  location_id TEXT PRIMARY KEY, zone TEXT, road_type TEXT, road_condition TEXT,
  population_density_per_km2 REAL, near_school INT, near_hospital INT,
  pedestrian_activity TEXT, latitude REAL, longitude REAL
);
CREATE TABLE IF NOT EXISTS incidents (
  incident_id TEXT PRIMARY KEY, location_id TEXT, timestamp TEXT, incident_type TEXT,
  severity TEXT, injuries INT, reported_by TEXT, resolved INT, resolution_hours REAL
);
CREATE TABLE IF NOT EXISTS complaints (
  complaint_id TEXT PRIMARY KEY, location_id TEXT, created_at TEXT, category TEXT,
  priority_reported TEXT, status TEXT, days_to_resolution REAL
);
CREATE INDEX IF NOT EXISTS idx_inc_loc ON incidents(location_id);
CREATE INDEX IF NOT EXISTS idx_inc_ts ON incidents(timestamp);
CREATE INDEX IF NOT EXISTS idx_cmp_loc ON complaints(location_id);
CREATE INDEX IF NOT EXISTS idx_cmp_ts ON complaints(created_at);
CREATE TABLE IF NOT EXISTS location_aggregates (
  location_id TEXT PRIMARY KEY, incident_count INT, complaint_count INT,
  sev_score REAL, recency_30d REAL, backlog INT, vulner REAL,
  critical_count INT, high_count INT, unresolved_incidents INT, open_complaints INT,
  top_incident_type TEXT, top_complaint_category TEXT, last_event_at TEXT
);
CREATE TABLE IF NOT EXISTS ranking_runs (
  id INTEGER PRIMARY KEY, version TEXT, params TEXT, created_at TEXT DEFAULT (datetime('now')),
  input_hash TEXT, status TEXT
);
CREATE TABLE IF NOT EXISTS ranking_results (
  run_id INT, location_id TEXT, priority REAL, rank INT,
  confidence TEXT, reason TEXT, missing_info TEXT,
  bayes_mean REAL, bayes_p_exceed REAL,
  PRIMARY KEY (run_id, location_id)
);
CREATE TABLE IF NOT EXISTS resources (
  id INTEGER PRIMARY KEY, name TEXT, crew_type TEXT, capacity_hours REAL, cost_per_hour REAL, zone TEXT
);
CREATE TABLE IF NOT EXISTS interventions (
  id INTEGER PRIMARY KEY, location_id TEXT, kind TEXT, title TEXT,
  est_cost REAL, est_hours REAL, exp_benefit REAL, effort TEXT, reason TEXT, confidence TEXT, status TEXT DEFAULT 'proposed'
);
CREATE TABLE IF NOT EXISTS work_orders (
  id INTEGER PRIMARY KEY, intervention_id INT, assignee TEXT, status TEXT DEFAULT 'open',
  created_at TEXT DEFAULT (datetime('now')), updated_at TEXT
);
CREATE TABLE IF NOT EXISTS scenarios (
  id INTEGER PRIMARY KEY, name TEXT, budget REAL, crew_hours REAL, result_json TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS decisions (
  id INTEGER PRIMARY KEY, location_id TEXT, intervention_id INT, action TEXT,
  actor TEXT, reason TEXT, before_score REAL, after_score REAL,
  created_at TEXT DEFAULT (datetime('now'))
);
"""

def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = get_db()
    con.executescript(SCHEMA)
    con.commit()
    # seed default crews so Resource Planner works with zero extra input
    cur = con.execute("SELECT COUNT(*) c FROM resources")
    if cur.fetchone()["c"] == 0:
        crews = [
            ("Road Crew A", "road", 160, 50, "ALL"),
            ("Drain Crew B", "drain", 120, 45, "ALL"),
            ("Electrical Crew C", "electrical", 120, 40, "ALL"),
            ("Signal Crew D", "signal", 80, 60, "ALL"),
            ("Safety Audit Team", "safety", 60, 70, "ALL"),
            ("Sanitation Team", "sanitation", 100, 30, "ALL"),
        ]
        con.executemany("INSERT INTO resources(name,crew_type,capacity_hours,cost_per_hour,zone) VALUES(?,?,?,?,?)", crews)
        con.commit()
    con.close()

if __name__ == "__main__":
    init_db()
    print(f"DB ready at {DB_PATH}")
