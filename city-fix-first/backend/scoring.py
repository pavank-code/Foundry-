"""Priority Engine: transparent score + Bayesian uncertainty + rules.
Formula:
  PRIORITY = 100*(0.30*norm(freq)+0.30*norm(sev)+0.15*recency+0.15*norm(backlog)+0.10*norm(vulner*pop))
Bayesian: Gamma-Poisson posterior mean for incident rate + P(lambda > critical).
"""
import json
import numpy as np
import pandas as pd
from models_db import get_db

W = dict(freq=0.30, sev=0.30, rec=0.15, backlog=0.15, vulner=0.10)
VERSION = "v1.0-rule+betalbayes"

INTERVENTION_RULES = [
    (("Pothole", "Road damage"), "Road repair crew", "Medium", 60, 24),
    (("Flooding", "Drainage"), "Drain desilting + pump check", "High", 120, 48),
    (("Blocked drain", "Drainage"), "Drain desilting + pump check", "High", 100, 40),
    (("Streetlight outage", "Lighting"), "Electrical maintenance", "Low", 30, 8),
    (("Signal failure", "Traffic"), "Signal engineering team", "Medium", 70, 20),
    (("Collision", "Pedestrian safety"), "Safety audit + speed calming", "High", 110, 36),
    (("waste", "Waste"), "Sanitation drive", "Low", 25, 12),
]

def _norm(s: pd.Series) -> pd.Series:
    mn, mx = s.min(), s.max()
    if mx == mn:
        return pd.Series(0.5, index=s.index)
    return (s - mn) / (mx - mn)

def compute_rankings() -> pd.DataFrame:
    con = get_db()
    agg = pd.read_sql("SELECT * FROM location_aggregates", con)
    loc = pd.read_sql("SELECT * FROM locations", con)
    con.close()
    df = agg.merge(loc, on="location_id")
    df["freq"] = df.incident_count + df.complaint_count
    df["vulner_pop"] = df.vulner * np.log1p(df.population_density_per_km2)
    df["n_freq"] = _norm(df.freq)
    df["n_sev"] = _norm(df.sev_score)
    df["n_back"] = _norm(df.backlog)
    df["n_vuln"] = _norm(df.vulner_pop)
    df["priority"] = 100 * (W["freq"] * df.n_freq + W["sev"] * df.n_sev + W["rec"] * df.recency_30d + W["backlog"] * df.n_back + W["vulner"] * df.n_vuln)
    # Bayesian Gamma-Poisson: prior alpha0=2, beta0=1 (per 12mo exposure=1yr)
    alpha0, beta0 = 2.0, 1.0
    df["bayes_mean"] = (alpha0 + df.freq) / (beta0 + 1.0)
    # P(lambda > critical=15/yr) via gamma survival approx (scipy-free)
    from math import gamma as Gamma
    def p_exceed(a, b, thr=15.0, n=2000):
        # numeric integrate gamma pdf tail
        xs = np.linspace(thr, thr + 60, n)
        pdf = b**a / Gamma(a) * xs**(a - 1) * np.exp(-b * xs)
        return float(np.trapezoid(pdf, xs))
    df["bayes_p_exceed"] = [p_exceed(a, beta0 + 1) for a in (alpha0 + df.freq)]
    df = df.sort_values("priority", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1
    # confidence + evidence strings
    def conf(r):
        tot = r.freq
        agree = (r.incident_count > 0 and r.complaint_count > 0)
        if tot >= 15 and agree: return "High"
        if tot >= 8: return "Medium"
        return "Low"
    df["confidence"] = df.apply(conf, axis=1)
    def reason(r):
        return (f"{r.incident_count} incidents ({r.critical_count} Critical, {r.high_count} High; top: {r.top_incident_type}) + "
                f"{r.complaint_count} complaints (top: {r.top_complaint_category}); {r.backlog} unresolved/open; "
                f"road {r.road_condition}, ped {r.pedestrian_activity}"
                + (", near school" if r.near_school else "") + (", near hospital" if r.near_hospital else ""))
    df["reason"] = df.apply(reason, axis=1)
    def missing(r):
        m = []
        if r.freq < 8: m.append("Fewer than 8 events — estimate is noisy")
        if r.incident_count == 0: m.append("No incident records yet")
        if r.complaint_count == 0: m.append("No complaint records yet")
        if r.last_event_at is None: m.append("No timestamped events")
        return "; ".join(m) if m else "None — evidence is sufficient"
    df["missing_info"] = df.apply(missing, axis=1)
    return df

def recommend_for_row(r) -> tuple[str, str, str, float, float]:
    it, cc = str(r.top_incident_type), str(r.top_complaint_category)
    for (a, b), title, effort, cost, hrs in INTERVENTION_RULES:
        if a.lower() in it.lower() or b.lower() in cc.lower():
            benefit = float(r.priority)  # expected harm/backlog reduction proxy
            return title, effort, f"Top signals {it}/{cc} map to {title}; score {r.priority:.1f}", cost, hrs
    return "General inspection", "Low", "No dominant pattern — inspect first", 20, 6

def persist_run(df: pd.DataFrame) -> int:
    import datetime
    con = get_db()
    run_id = con.execute("INSERT INTO ranking_runs(version,params,status) VALUES(?,?,?)",
                         (VERSION, json.dumps(W), "done")).lastrowid
    rows = [(run_id, r.location_id, float(r.priority), int(r.rank), r.confidence, r.reason, r.missing_info,
             float(r.bayes_mean), float(r.bayes_p_exceed)) for r in df.itertuples()]
    con.executemany("INSERT OR REPLACE INTO ranking_results(run_id,location_id,priority,rank,confidence,reason,missing_info,bayes_mean,bayes_p_exceed) VALUES(?,?,?,?,?,?,?,?,?)", rows)
    # interventions
    con.execute("DELETE FROM interventions")
    for r in df.itertuples():
        title, effort, reason, cost, hrs = recommend_for_row(r)
        con.execute("""INSERT INTO interventions(location_id,kind,title,est_cost,est_hours,exp_benefit,effort,reason,confidence)
          VALUES(?,?,?,?,?,?,?,?,?)""", (r.location_id, title, f"{title} @ {r.location_id}", cost, hrs, float(r.priority), effort, reason, r.confidence))
    con.commit()
    con.close()
    return run_id

if __name__ == "__main__":
    df = compute_rankings()
    rid = persist_run(df)
    print(f"run {rid} saved, top5:")
    print(df[["rank", "location_id", "priority", "confidence"]].head(5).to_string(index=False))
