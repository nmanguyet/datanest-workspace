##
# """usage_examples.py — Demo Tracker (2-sheet model, no hardcoded data)."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import pandas as pd
pd.set_option("display.max_colwidth", 50)
pd.set_option("display.width", 200)
from tracking import Tracker

t = Tracker(os.path.join(os.path.dirname(__file__), "client_model_tracking_v2.xlsx"))
print(t); print()

print("=" * 80)
print("USE CASE 1: VNPT x HDBANK @ 2025-06-01 — full legacy IDs")
print("=" * 80)
r = t.query(date="2025-06-01", telco="VNPT", client="HDBANK")
print(r[["client", "client_product", "client_model_code",
         "target_score_table", "hdfs_real_model_code"]].to_string(index=False))
print()

print("=" * 80)
print("USE CASE 2: All VT clients active @ 2025-06-01")
print("=" * 80)
r = t.summary(date="2025-06-01", telco="VT")
print(r.to_string(index=False))
print()

print("=" * 80)
print("USE CASE 3: All deployments active today (null date_end counts)")
print("=" * 80)
r = t.summary()
print(f"Total: {len(r)} active deployments")
print(r.groupby('telco').size().to_string())
print()

print("=" * 80)
print("USE CASE 4: MBF × KREDIVO history (chronological, including inactive)")
print("=" * 80)
r = t.query(date=None, telco="MBF", client="KREDIVO", active_only=False)
print(r[["client", "client_model_code", "target_score_table",
         "hdfs_real_model_code", "date_start", "date_end"]].to_string(index=False))
print()

print("=" * 80)
print("USE CASE 5: Resolve a model_code — full metadata from model_catalog")
print("=" * 80)
meta = t.resolve("vnpt_homecredit__cs_generic__v5.1")
for k, v in (meta or {}).items():
    print(f"  {k:22s}: {v}")
print()

print("=" * 80)
print("USE CASE 6: Resolve a legacy-tagged code via base derivation")
print("=" * 80)
code = "vt_kredivo__is_numeric__v1.1_[is_numeric_v1.2]"
meta = t.resolve(code)
print(f"  Input: {code}")
print(f"  → base: {meta['client_model_code'] if meta else None}")
print(f"  → family: {meta['model_family'] if meta else None}")
print(f"  → description: {meta['model_description'] if meta else None}")
print()

print("=" * 80)
print("USE CASE 7: Refresh tracking sheet")
print("=" * 80)
n = t.refresh()
print(f"  Rebuilt {n} rows in `tracking` sheet")

# %%
