import sys
sys.path.insert(0, "city-fix-first/backend")
from fastapi.testclient import TestClient
import main
c = TestClient(main.app)
print("graph:", len(c.get("/api/graph/L0362").json()["nodes"]), "nodes")
print("compare:", c.get("/api/compare", params={"a": "L0362", "b": "L0965"}).json()["verdict"])
print("pipeline:", len(c.get("/api/pipeline").json()["batches"]), "batches")
print("trust:", c.get("/api/trust", params={"location_id": "L0362"}).json())
print("copilot:", c.get("/api/copilot", params={"q": "why L0362?"}).json()["answer"])
print("assign:", c.post("/api/actions/assign", params={"intervention_id": 1, "assignee": "Road Crew A"}).json())
print("complete:", c.post("/api/actions/complete", params={"work_order_id": 1}).json())
print("scen:", c.post("/api/scenarios", params={"name": "demo", "budget": 600, "crew_hours": 300}).json()["covered_top20"])
