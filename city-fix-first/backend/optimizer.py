"""Constrained allocation: max benefit subject to budget + crew hours (greedy knapsack)."""
import pandas as pd
from models_db import get_db

def allocate(budget: float = 1000, crew_hours: float = 400):
    con = get_db()
    iv = pd.read_sql("""SELECT i.*, r.priority, r.rank FROM interventions i
      JOIN ranking_results r ON r.location_id=i.location_id
      WHERE r.run_id=(SELECT MAX(id) FROM ranking_runs) ORDER BY r.rank""", con)
    con.close()
    # greedy by benefit/cost
    iv["ratio"] = iv.exp_benefit / iv.est_cost.clip(lower=1)
    iv = iv.sort_values("ratio", ascending=False)
    picked, spent, hrs = [], 0.0, 0.0
    for _, t in iv.iterrows():
        if spent + t.est_cost <= budget and hrs + t.est_hours <= crew_hours:
            picked.append(t)
            spent += t.est_cost
            hrs += t.est_hours
    rows = [{"location_id": p["location_id"], "title": p["title"], "est_cost": float(p["est_cost"]),
             "est_hours": float(p["est_hours"]), "exp_benefit": float(p["exp_benefit"]), "rank": int(p["rank"])} for p in picked]
    uncovered = int((iv["rank"] <= 20).sum() - sum(1 for r in rows if r["rank"] <= 20))
    return {"picked": rows, "spent": spent, "hours_used": hrs,
            "covered_top20": sum(1 for r in rows if r["rank"] <= 20),
            "uncovered_top20": max(uncovered, 0)}
