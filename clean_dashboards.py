"""Prepare exported Grafana dashboards for file provisioning.

For every JSON file in exports/:
- skips files still in the v2 format (they need re-exporting as Classic)
- sets "id" to null (provisioning assigns its own id)
- sets every variable that has an "All" option to default to All
- saves it to grafana/dashboards/<title-as-slug>.json
"""
import json
import re
from pathlib import Path

SRC = Path("exports")
DST = Path("grafana/dashboards")
DST.mkdir(parents=True, exist_ok=True)

for path in sorted(SRC.glob("*.json")):
    dashboard = json.loads(path.read_text())

    if "apiVersion" in dashboard:
        print(f"SKIPPED {path.name}: v2 format, re-export it as Classic")
        continue

    dashboard["id"] = None

    for var in dashboard.get("templating", {}).get("list", []):
        if var.get("includeAll"):
            if var.get("multi"):
                var["current"] = {"text": ["All"], "value": ["$__all"]}
            else:
                var["current"] = {"text": "All", "value": "$__all"}

    slug = re.sub(r"[^a-z0-9]+", "-", dashboard["title"].lower()).strip("-")
    out = DST / f"{slug}.json"
    out.write_text(json.dumps(dashboard, indent=2))
    print(f"{path.name}  ->  {out}")