"""FastAPI: real endpoints over city.db — no mock data."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from models_db import get_db
from optimizer import allocate

app = FastAPI(title="Where Should We Fix First")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/rankings")
def rankings(zone: str = "", limit: int = 100):
    con = get_db()
    q = """SELECT r.*, l.zone, l.latitude, l.longitude, l.road_condition, l.road_type,
      l.population_density_per_km2, l.near_school, l.near_hospital, l.pedestrian_activity,
      a.incident_count, a.complaint_count, a.backlog, a.top_incident_type, a.top_complaint_category
      FROM ranking_results r JOIN locations l ON l.location_id=r.location_id
      JOIN location_aggregates a ON a.location_id=r.location_id
      WHERE r.run_id=(SELECT MAX(id) FROM ranking_runs)"""
    if zone:
        q += " AND l.zone='" + zone.replace("'", "") + "'"
    q += " ORDER BY r.rank"
    df = pd.read_sql(q, con)
    con.close()
    return df.head(limit).to_dict(orient="records")

@app.get("/api/locations/{lid}")
def location_detail(lid: str):
    con = get_db()
    loc = pd.read_sql("SELECT * FROM locations WHERE location_id=?", con, params=[lid])
    agg = pd.read_sql("SELECT * FROM location_aggregates WHERE location_id=?", con, params=[lid])
    rk = pd.read_sql("SELECT * FROM ranking_results WHERE location_id=? AND run_id=(SELECT MAX(id) FROM ranking_runs)", con, params=[lid])
    inc = pd.read_sql("SELECT * FROM incidents WHERE location_id=? ORDER BY timestamp DESC LIMIT 50", con, params=[lid])
    cmp_ = pd.read_sql("SELECT * FROM complaints WHERE location_id=? ORDER BY created_at DESC LIMIT 50", con, params=[lid])
    iv = pd.read_sql("SELECT * FROM interventions WHERE location_id=?", con, params=[lid])
    con.close()
    if loc.empty:
        return {"error": "not found"}
    return {"location": loc.iloc[0].to_dict(),
            "aggregate": agg.iloc[0].to_dict() if not agg.empty else {},
            "ranking": rk.iloc[0].to_dict() if not rk.empty else {},
            "incidents": inc.to_dict(orient="records"),
            "complaints": cmp_.to_dict(orient="records"),
            "interventions": iv.to_dict(orient="records")}

@app.get("/api/trends")
def trends(group: str = "week"):
    con = get_db()
    inc = pd.read_sql("SELECT timestamp, incident_type FROM incidents", con, parse_dates=["timestamp"])
    cmp_ = pd.read_sql("SELECT created_at, category FROM complaints", con, parse_dates=["created_at"])
    con.close()
    rule = "W" if group == "week" else ("ME" if group == "month" else "D")
    it = inc.groupby([pd.Grouper(key="timestamp", freq=rule), "incident_type"]).size().unstack(fill_value=0)
    ct = cmp_.groupby([pd.Grouper(key="created_at", freq=rule), "category"]).size().unstack(fill_value=0)
    out = [{"t": str(i), **{k: int(v) for k, v in row.items()}} for i, row in it.iterrows()]
    out2 = [{"t": str(i), **{k: int(v) for k, v in row.items()}} for i, row in ct.iterrows()]
    tot = it.sum(axis=1)
    th = tot.mean() + 2 * tot.std()
    anoms = [str(i) for i in tot[tot > th].index]
    return {"incidents": out, "complaints": out2, "anomaly_weeks": anoms, "threshold": float(th)}

@app.get("/api/resolution")
def resolution():
    con = get_db()
    inc = pd.read_sql("SELECT incident_type, resolved, resolution_hours FROM incidents", con)
    cmp_ = pd.read_sql("SELECT category, status, days_to_resolution FROM complaints", con)
    zones = pd.read_sql("SELECT l.zone, AVG(i.resolution_hours) avg_h FROM incidents i JOIN locations l ON l.location_id=i.location_id GROUP BY l.zone", con)
    con.close()
    return {
        "incident_avg_hours": float(inc.resolution_hours.mean()),
        "incident_median_hours": float(inc.resolution_hours.median()),
        "complaint_avg_days": float(cmp_.days_to_resolution.mean()),
        "complaint_median_days": float(cmp_.days_to_resolution.median()),
        "by_type": inc.groupby("incident_type").resolution_hours.mean().round(1).to_dict(),
        "by_category": cmp_.groupby("category").days_to_resolution.mean().round(2).to_dict(),
        "backlog_funnel": cmp_.status.value_counts().to_dict(),
        "by_zone": zones.to_dict(orient="records"),
    }

@app.get("/api/recommendations")
def recommendations(limit: int = 50):
    con = get_db()
    df = pd.read_sql("SELECT i.*, r.priority, r.rank, r.confidence FROM interventions i JOIN ranking_results r ON r.location_id=i.location_id WHERE r.run_id=(SELECT MAX(id) FROM ranking_runs) ORDER BY r.rank LIMIT ?", con, params=[limit])
    con.close()
    return df.to_dict(orient="records")

@app.post("/api/resources/allocate")
def allocate_api(budget: float = 1000, crew_hours: float = 400):
    return allocate(budget, crew_hours)

@app.post("/api/decisions")
def decide(location_id: str, action: str, actor: str = "officer", reason: str = ""):
    con = get_db()
    rk = pd.read_sql("SELECT priority FROM ranking_results WHERE location_id=? AND run_id=(SELECT MAX(id) FROM ranking_runs)", con, params=[location_id])
    before = float(rk.priority.iloc[0]) if not rk.empty else 0
    cur = con.execute("INSERT INTO decisions(location_id,action,actor,reason,before_score) VALUES(?,?,?,?,?)", (location_id, action, actor, reason, before)).lastrowid
    con.commit()
    con.close()
    return {"decision_id": cur, "before_score": before, "receipt": "done"}


@app.get("/api/lineage")
def lineage(location_id: str):
    con = get_db()
    rk = pd.read_sql("SELECT * FROM ranking_results WHERE location_id=? ORDER BY run_id DESC LIMIT 3", con, params=[location_id])
    agg = pd.read_sql("SELECT * FROM location_aggregates WHERE location_id=?", con, params=[location_id])
    run = pd.read_sql("SELECT * FROM ranking_runs ORDER BY id DESC LIMIT 1", con)
    con.close()
    return {"runs": rk.to_dict(orient="records"),
            "aggregate": agg.to_dict(orient="records")[0] if len(agg) else {},
            "current_version": run.iloc[0].to_dict() if len(run) else {}}

@app.get("/api/zones")
def zones():
    con = get_db()
    df = pd.read_sql("SELECT l.zone, COUNT(*) n_loc, AVG(r.priority) avg_priority, SUM(a.backlog) backlog FROM ranking_results r JOIN locations l ON l.location_id=r.location_id JOIN location_aggregates a ON a.location_id=r.location_id WHERE r.run_id=(SELECT MAX(id) FROM ranking_runs) GROUP BY l.zone ORDER BY avg_priority DESC", con)
    con.close()
    return df.to_dict(orient="records")

@app.get("/api/filters/options")
def options():
    con = get_db()
    df = pd.read_sql("SELECT DISTINCT zone FROM locations ORDER BY 1", con)
    it = pd.read_sql("SELECT DISTINCT incident_type FROM incidents", con)
    ct = pd.read_sql("SELECT DISTINCT category FROM complaints", con)
    con.close()
    return {"zones": df.zone.tolist(), "incident_types": it.incident_type.tolist(), "categories": ct.category.tolist()}

# FEATURE 1: Civic Operations Graph
@app.get("/api/graph/{lid}")
def civic_graph(lid: str):
    con = get_db()
    loc = pd.read_sql("SELECT * FROM locations WHERE location_id=?", con, params=[lid])
    if loc.empty:
        con.close()
        return {"error": "not found"}
    rk = pd.read_sql("SELECT * FROM ranking_results WHERE location_id=? AND run_id=(SELECT MAX(id) FROM ranking_runs)", con, params=[lid])
    inc = pd.read_sql("SELECT incident_id, incident_type, severity, resolved FROM incidents WHERE location_id=? LIMIT 30", con, params=[lid])
    cmp_ = pd.read_sql("SELECT complaint_id, category, status FROM complaints WHERE location_id=? LIMIT 30", con, params=[lid])
    iv = pd.read_sql("SELECT * FROM interventions WHERE location_id=?", con, params=[lid])
    wo = pd.read_sql("SELECT w.* FROM work_orders w JOIN interventions i ON i.id=w.intervention_id WHERE i.location_id=?", con, params=[lid])
    dec = pd.read_sql("SELECT * FROM decisions WHERE location_id=? ORDER BY id DESC LIMIT 10", con, params=[lid])
    res = pd.read_sql("SELECT * FROM resources", con)
    con.close()
    nodes = [{"id": lid, "kind": "location", "label": lid}]
    edges = []
    for t in inc.itertuples():
        nodes.append({"id": t.incident_id, "kind": "incident", "label": str(t.incident_type) + "/" + str(t.severity)})
        edges.append({"from": lid, "to": t.incident_id, "rel": "has_incident"})
    for t in cmp_.itertuples():
        nodes.append({"id": t.complaint_id, "kind": "complaint", "label": str(t.category) + "/" + str(t.status)})
        edges.append({"from": lid, "to": t.complaint_id, "rel": "has_complaint"})
    for t in iv.itertuples():
        nodes.append({"id": "IV" + str(t.id), "kind": "intervention", "label": t.title})
        edges.append({"from": lid, "to": "IV" + str(t.id), "rel": "proposed_fix"})
    return {"location": loc.iloc[0].to_dict(), "ranking": rk.iloc[0].to_dict() if len(rk) else {},
            "nodes": nodes, "edges": edges,
            "work_orders": wo.to_dict(orient="records"), "decisions": dec.to_dict(orient="records"),
            "resources": res.to_dict(orient="records")}

# FEATURE 4: Location Explorer compare
@app.get("/api/compare")
def compare(a: str, b: str):
    con = get_db()
    q = "SELECT r.location_id, r.priority, r.rank, r.confidence, r.reason, r.bayes_p_exceed, l.zone, l.road_condition, l.road_type, a.incident_count, a.complaint_count, a.sev_score, a.recency_30d, a.backlog, a.top_incident_type, a.top_complaint_category FROM ranking_results r JOIN locations l ON l.location_id=r.location_id JOIN location_aggregates a ON a.location_id=r.location_id WHERE r.run_id=(SELECT MAX(id) FROM ranking_runs) AND r.location_id IN (?,?)"
    df = pd.read_sql(q, con, params=[a, b])
    con.close()
    rows = {r["location_id"]: r for r in df.to_dict(orient="records")}
    if a not in rows or b not in rows:
        return {"error": "one or both locations not found"}
    ra, rb = rows[a], rows[b]
    diffs = []
    for k in ["priority", "incident_count", "complaint_count", "sev_score", "backlog"]:
        diffs.append({"field": k, "a": ra[k], "b": rb[k], "delta": round(float(ra[k]) - float(rb[k]), 2)})
    verdict = str(a) + " rank " + str(ra["rank"]) + " vs " + str(b) + " rank " + str(rb["rank"]) + " because freq " + str(ra["incident_count"] + ra["complaint_count"]) + " vs " + str(rb["incident_count"] + rb["complaint_count"]) + ", sev " + str(round(ra["sev_score"])) + " vs " + str(round(rb["sev_score"])) + ", backlog " + str(ra["backlog"]) + " vs " + str(rb["backlog"]) + "."
    return {"a": ra, "b": rb, "diffs": diffs, "verdict": verdict}

# FEATURE 5+8: Pipeline Mission Control + Trust
@app.get("/api/pipeline")
def pipeline():
    con = get_db()
    src = pd.read_sql("SELECT * FROM data_sources", con)
    batches = pd.read_sql("SELECT * FROM ingestion_batches ORDER BY id DESC LIMIT 5", con)
    issues = pd.read_sql("SELECT * FROM validation_issues ORDER BY id DESC LIMIT 50", con)
    runs = pd.read_sql("SELECT * FROM ranking_runs ORDER BY id DESC LIMIT 5", con)
    mx1 = pd.read_sql("SELECT MAX(timestamp) m FROM incidents", con).iloc[0]["m"]
    mx2 = pd.read_sql("SELECT MAX(created_at) m FROM complaints", con).iloc[0]["m"]
    con.close()
    return {"sources": src.to_dict(orient="records"), "batches": batches.to_dict(orient="records"),
            "issues": issues.to_dict(orient="records"), "runs": runs.to_dict(orient="records"),
            "freshness": {"incidents_max": mx1, "complaints_max": mx2}}

@app.get("/api/trust")
def trust(location_id: str = ""):
    con = get_db()
    if location_id:
        agg = pd.read_sql("SELECT * FROM location_aggregates WHERE location_id=?", con, params=[location_id])
        rk = pd.read_sql("SELECT * FROM ranking_results WHERE location_id=? AND run_id=(SELECT MAX(id) FROM ranking_runs)", con, params=[location_id])
        con.close()
        if agg.empty:
            return {"error": "not found"}
        a = agg.iloc[0].to_dict()
        tot = a["incident_count"] + a["complaint_count"]
        flags = []
        if tot < 8:
            flags.append("LOW_VOLUME: estimate is noisy")
        if a["incident_count"] == 0:
            flags.append("NO_INCIDENTS")
        if a["complaint_count"] == 0:
            flags.append("NO_COMPLAINTS")
        if a["backlog"] > 10:
            flags.append("HIGH_BACKLOG: verify field state")
        return {"location_id": location_id, "total_events": int(tot), "flags": flags,
                "confidence": rk.iloc[0]["confidence"] if len(rk) else "unknown",
                "last_event_at": a["last_event_at"]}

# FEATURE 3: Operational Actions with state change + audit
@app.post("/api/actions/assign")
def assign(intervention_id: int, assignee: str, actor: str = "officer"):
    con = get_db()
    cur = con.execute("INSERT INTO work_orders(intervention_id, assignee, status) VALUES(?,?,'assigned')", (intervention_id, assignee)).lastrowid
    con.execute("UPDATE interventions SET status='assigned' WHERE id=?", (intervention_id,))
    iv = pd.read_sql("SELECT location_id FROM interventions WHERE id=?", con, params=[intervention_id])
    lid = iv.iloc[0]["location_id"] if len(iv) else ""
    con.execute("INSERT INTO decisions(location_id,intervention_id,action,actor,reason) VALUES(?,?,?,?,?)", (lid, intervention_id, "assign", actor, "assigned to " + assignee))
    con.commit()
    con.close()
    return {"work_order_id": cur, "status": "assigned"}

@app.post("/api/actions/complete")
def complete_wo(work_order_id: int, actor: str = "officer"):
    con = get_db()
    con.execute("UPDATE work_orders SET status='completed', updated_at=datetime('now') WHERE id=?", (work_order_id,))
    con.execute("UPDATE interventions SET status='completed' WHERE id=(SELECT intervention_id FROM work_orders WHERE id=?)", (work_order_id,))
    con.execute("INSERT INTO decisions(location_id,action,actor,reason) SELECT i.location_id,'complete',?,'WO ' || ? FROM work_orders w JOIN interventions i ON i.id=w.intervention_id WHERE w.id=?", (actor, str(work_order_id), work_order_id))
    con.commit()
    con.close()
    return {"work_order_id": work_order_id, "status": "completed"}

@app.get("/api/scenarios")
def list_scenarios():
    con = get_db()
    try:
        df = pd.read_sql("SELECT * FROM scenarios ORDER BY id DESC", con)
    except Exception:
        df = pd.DataFrame()
    con.close()
    return df.to_dict(orient="records") if len(df) else []

@app.post("/api/scenarios")
def save_scenario(name: str, budget: float = 1000, crew_hours: float = 400):
    import json as _j
    res = allocate(budget, crew_hours)
    con = get_db()
    cur = con.execute("INSERT INTO scenarios(name,budget,crew_hours,result_json) VALUES(?,?,?,?)",
                      (name, budget, crew_hours, _j.dumps(res))).lastrowid
    con.commit()
    con.close()
    return {"scenario_id": cur, **res}

# FEATURE 6: Context-Aware Operations Copilot (deterministic, grounded)
@app.get("/api/copilot")
def copilot(q: str):
    """Answer from live DB only. Evidence vs inference separated. No hallucinated numbers."""
    import re
    con = get_db()
    ql = q.lower()
    out = {"question": q, "answer": "", "evidence": [], "inference": ""}
    m = re.findall(r"L\d{4}", q.upper())
    if "school" in ql and ("unresolv" in ql or "near" in ql or "which" in ql):
        df = pd.read_sql("SELECT r.location_id, r.priority, r.rank, a.backlog FROM ranking_results r JOIN locations l ON l.location_id=r.location_id JOIN location_aggregates a ON a.location_id=r.location_id WHERE r.run_id=(SELECT MAX(id) FROM ranking_runs) AND l.near_school=1 AND a.backlog>0 ORDER BY r.rank LIMIT 10", con)
        out["answer"] = f"{len(df)} unresolved locations near schools in top queue."
        out["evidence"] = df.to_dict(orient="records")
        out["inference"] = "Prioritize school-adjacent backlog first (policy choice, not model output)."
    elif m:
        lid = m[0]
        rk = pd.read_sql("SELECT * FROM ranking_results WHERE location_id=? AND run_id=(SELECT MAX(id) FROM ranking_runs)", con, params=[lid])
        if len(rk):
            r = rk.iloc[0].to_dict()
            out["answer"] = str(lid) + " is rank " + str(r["rank"]) + " with priority " + str(round(float(r["priority"]), 1)) + " (" + str(r["confidence"]) + " confidence)."
            out["evidence"] = [{"reason": r["reason"], "missing_info": r["missing_info"], "bayes_p_exceed": r["bayes_p_exceed"]}]
            out["inference"] = "Rank follows transparent weights; check missing_info before acting."
        else:
            out["answer"] = lid + " not found."
    elif "zone" in ql:
        df = pd.read_sql("SELECT l.zone, COUNT(*) n_loc, AVG(r.priority) avg_priority, SUM(a.backlog) backlog FROM ranking_results r JOIN locations l ON l.location_id=r.location_id JOIN location_aggregates a ON a.location_id=r.location_id WHERE r.run_id=(SELECT MAX(id) FROM ranking_runs) GROUP BY l.zone ORDER BY avg_priority DESC LIMIT 5", con)
        out["answer"] = "Top zones by avg priority: " + ", ".join(df.zone.tolist())
        out["evidence"] = df.to_dict(orient="records")
        out["inference"] = "Zone avg hides within-zone variance; inspect location ranks."
    else:
        df = pd.read_sql("SELECT location_id, priority, rank FROM ranking_results WHERE run_id=(SELECT MAX(id) FROM ranking_runs) ORDER BY rank LIMIT 5", con)
        out["answer"] = "Top 5 locations by priority (live): " + ", ".join(df.location_id.tolist())
        out["evidence"] = df.to_dict(orient="records")
        out["inference"] = "Ask 'why L0362?' or 'which unresolved near schools?' for specifics."
    con.close()
    return out



    agg = pd.read_sql("SELECT COUNT(*) n FROM location_aggregates", con)
    iss = pd.read_sql("SELECT COUNT(*) c FROM validation_issues WHERE status='open'", con)
    con.close()
    return {"locations": int(agg.iloc[0]["n"]), "open_validation_issues": int(iss.iloc[0]["c"])}

    return {"a": ra, "b": rb, "diffs": diffs, "verdict": verdict}

    return {"location": loc.iloc[0].to_dict(), "ranking": rk.iloc[0].to_dict() if len(rk) else {},
            "nodes": nodes, "edges": edges,
            "work_orders": wo.to_dict(orient="records"), "decisions": dec.to_dict(orient="records"),
            "resources": res.to_dict(orient="records")}


