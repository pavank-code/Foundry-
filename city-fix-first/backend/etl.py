"""ETL: CSVs -> SQLite canonical tables with validation + quarantine."""
import hashlib, json, sqlite3
from pathlib import Path
import pandas as pd

BASE = Path(__file__).parent
DATA = BASE / ".." / ".." / "03_smart_city_where_fix_first"
DATA = DATA.resolve()
from models_db import init_db, get_db

SEV = {"Low": 1, "Medium": 2, "High": 4, "Critical": 8}

def h(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()[:16]

def load():
    init_db()
    con = get_db()
    loc = pd.read_csv(DATA / "locations.csv")
    inc = pd.read_csv(DATA / "incidents.csv", parse_dates=["timestamp"])
    cmp_ = pd.read_csv(DATA / "public_complaints.csv", parse_dates=["created_at"])

    print(f"rows: locations={len(loc)} incidents={len(inc)} complaints={len(cmp_)}")
    print("incident types:", inc.incident_type.value_counts().to_dict())
    print("severity:", inc.severity.value_counts().to_dict())
    print("complaint cats:", cmp_.category.value_counts().to_dict())
    print("status:", cmp_.status.value_counts().to_dict())

    # register sources + batch
    for name, path in [("locations", "locations.csv"), ("incidents", "incidents.csv"), ("complaints", "public_complaints.csv")]:
        con.execute("INSERT OR IGNORE INTO data_sources(name,kind,path) VALUES(?,?,?)", (name, "csv", path))
    src = {r["name"]: r["id"] for r in con.execute("SELECT * FROM data_sources")}
    b = con.execute("INSERT INTO ingestion_batches(source_id,status,rows_total) VALUES(?,?,?)", (src["incidents"], "ok", len(inc) + len(cmp_) + len(loc))).lastrowid

    # validation: unknown location_id refs get quarantined (kept out of canonical)
    valid = set(loc.location_id)
    bad_inc = inc[~inc.location_id.isin(valid)]
    bad_cmp = cmp_[~cmp_.location_id.isin(valid)]
    for _, r in bad_inc.iterrows():
        con.execute("INSERT INTO validation_issues(batch_id,source_table,source_key,rule_code,field,reason) VALUES(?,?,?,?,?,?)",
                    (b, "incidents", r.incident_id, "FK_LOCATION", "location_id", f"unknown {r.location_id}"))
    for _, r in bad_cmp.iterrows():
        con.execute("INSERT INTO validation_issues(batch_id,source_table,source_key,rule_code,field,reason) VALUES(?,?,?,?,?,?)",
                    (b, "complaints", r.complaint_id, "FK_LOCATION", "location_id", f"unknown {r.location_id}"))
    inc = inc[inc.location_id.isin(valid)]
    cmp_ = cmp_[cmp_.location_id.isin(valid)]

    loc.to_sql("locations", con, if_exists="replace", index=False)
    inc.assign(timestamp=inc.timestamp.dt.strftime("%Y-%m-%d %H:%M:%S")).to_sql("incidents", con, if_exists="replace", index=False)
    cmp_.assign(created_at=cmp_.created_at.dt.strftime("%Y-%m-%d %H:%M:%S")).to_sql("complaints", con, if_exists="replace", index=False)
    con.execute("UPDATE ingestion_batches SET rows_accepted=?, rows_quarantined=?, status='done' WHERE id=?",
                (len(inc) + len(cmp_) + len(loc), len(bad_inc) + len(bad_cmp), b))
    con.commit()

    # location_aggregates — evidence rollups
    inc["sev_w"] = inc.severity.map(SEV).fillna(1) + inc.injuries.fillna(0) * 2
    max_ts = max(inc.timestamp.max(), cmp_.created_at.max())
    cutoff30 = max_ts - pd.Timedelta(days=30)
    agg = []
    g_inc = inc.groupby("location_id")
    g_cmp = cmp_.groupby("location_id")
    for lid, lrow in loc.set_index("location_id").iterrows():
        gi = g_inc.get_group(lid) if lid in g_inc.groups else None
        gc = g_cmp.get_group(lid) if lid in g_cmp.groups else None
        ic = 0 if gi is None else len(gi)
        cc = 0 if gc is None else len(gc)
        sev = 0 if gi is None else float(gi.sev_w.sum())
        crit = 0 if gi is None else int((gi.severity == "Critical").sum())
        high = 0 if gi is None else int((gi.severity == "High").sum())
        unr = 0 if gi is None else int((gi.resolved == 0).sum())
        openc = 0 if gc is None else int(gc.status.isin(["Open", "Assigned", "In progress"]).sum())
        ev30 = (0 if gi is None else int((gi.timestamp >= cutoff30).sum())) + (0 if gc is None else int((gc.created_at >= cutoff30).sum()))
        tot = ic + cc
        rec = ev30 / tot if tot else 0
        vulner = 1 + 0.2 * lrow.near_school + 0.2 * lrow.near_hospital + 0.15 * (lrow.pedestrian_activity == "High") + 0.15 * (lrow.road_condition in ("Poor", "Critical"))
        top_it = None if gi is None else gi.incident_type.mode().iloc[0]
        top_cc = None if gc is None else gc.category.mode().iloc[0]
        last_ev = None
        if gi is not None or gc is not None:
            ts = []
            if gi is not None: ts.append(gi.timestamp.max())
            if gc is not None: ts.append(gc.created_at.max())
            last_ev = str(max(ts))
        agg.append((lid, ic, cc, sev, rec, unr + openc, vulner, crit, high, unr, openc, top_it, top_cc, last_ev))
    con.executemany("""INSERT OR REPLACE INTO location_aggregates(location_id,incident_count,complaint_count,sev_score,
      recency_30d,backlog,vulner,critical_count,high_count,unresolved_incidents,open_complaints,
      top_incident_type,top_complaint_category,last_event_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", agg)
    con.commit()
    print(f"aggregates={len(agg)} quarantined={len(bad_inc)+len(bad_cmp)} max_ts={max_ts}")
    con.close()

if __name__ == "__main__":
    load()
