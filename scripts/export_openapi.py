"""Write the engine's OpenAPI document (hand-off artifact for the platform team)."""

import json
import sys

from engine.api.app import build_app
from engine.config import Settings

out = sys.argv[1] if len(sys.argv) > 1 else "openapi.json"
app = build_app(Settings(_env_file=None))
with open(out, "w", encoding="utf-8") as f:
    json.dump(app.openapi(), f, indent=2)
print(f"wrote {out}")
