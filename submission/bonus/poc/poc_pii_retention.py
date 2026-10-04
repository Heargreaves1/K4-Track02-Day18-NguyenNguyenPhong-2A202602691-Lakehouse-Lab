# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # Bonus PoC — PII tokenization at landing + 7-day physical retention
#
# Spike for `ARCHITECTURE.md` (topic A). Proves the two hardest mechanisms:
# 1. Raw PII never reaches a file on disk (tokenized *before* the Bronze write),
#    while tokens stay joinable (same value → same token, per tenant).
# 2. Payload older than 7 days is unreadable even via time travel after
#    partition delete + VACUUM, while Gold aggregates survive.
#
# Run from the repo root with the lab venv: `python submission/bonus/poc/poc_pii_retention.py`

# %%
import hashlib
import hmac
import random
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, WriterProperties, write_deltalake

random.seed(18)
ROOT = Path(tempfile.mkdtemp(prefix="poc_llm_obs_"))
BRONZE, GOLD = str(ROOT / "bronze_payload"), str(ROOT / "gold_daily")
MASTER_KEY = b"demo-master-key-from-kms"   # production: fetched from KMS, never in code
N_DAYS, ROWS_PER_DAY, TENANTS = 10, 10_000, [f"t{i:03d}" for i in range(50)]

# %% [markdown]
# ## 1. Synthetic traffic with planted PII (email, VN phone, 12-digit CCCD)

# %%
emails = [f"user{i}@example.vn" for i in range(300)]
phones = [f"09{random.randint(10_000_000, 99_999_999)}" for _ in range(300)]
cccds = [f"0792{random.randint(10_000_000, 99_999_999)}" for _ in range(300)]
planted = set(emails) | set(phones) | set(cccds)

start = datetime(2026, 9, 25, tzinfo=timezone.utc)
rows = []
for d in range(N_DAYS):
    for i in range(ROWS_PER_DAY):
        ts = start + timedelta(days=d, seconds=i * 86_400 // ROWS_PER_DAY)
        prompt = (f"Contact {random.choice(emails)} or {random.choice(phones)}, "
                  f"ID {random.choice(cccds)}. Summarise invoice #{random.randint(1, 9999)}.")
        rows.append({"request_id": f"r{d:02d}{i:05d}", "ts": ts, "tenant_id": random.choice(TENANTS),
                     "latency_ms": random.randint(200, 4000), "cost_usd": random.random() / 100,
                     "prompt": prompt, "response": f"Done. Will email {random.choice(emails)}."})
print(f"generated {len(rows):,} requests over {N_DAYS} days, {len(planted)} distinct PII values")

# %% [markdown]
# ## 2. Tokenize in the stream, before landing
#
# Keyed HMAC per tenant: irreversible without the key, deterministic inside a
# tenant (joinable for incident review), not linkable across tenants.

# %%
PII = [("CCCD", re.compile(r"\b\d{12}\b")),
       ("PHONE", re.compile(r"\b(?:\+84|0)[35789]\d{8}\b")),
       ("EMAIL", re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"))]


def tenant_key(tenant: str) -> bytes:
    return hmac.new(MASTER_KEY, tenant.encode(), hashlib.sha256).digest()


def tokenize(text: str, tenant: str) -> str:
    key = tenant_key(tenant)
    for kind, rx in PII:
        text = rx.sub(lambda m: f"<{kind}:{hmac.new(key, m.group().encode(), hashlib.sha256).hexdigest()[:12]}>", text)
    return text


for r in rows:
    r["prompt"], r["response"] = tokenize(r["prompt"], r["tenant_id"]), tokenize(r["response"], r["tenant_id"])
    r["event_date"] = r["ts"].astimezone(timezone.utc).strftime("%Y-%m-%d")   # UTC, never session TZ
print("sample:", rows[0]["prompt"])

bronze = pa.Table.from_pylist(rows)
write_deltalake(BRONZE, bronze, mode="overwrite", partition_by=["event_date"])
# 64 KB files so each day keeps ~10 of them (delta-rs rolls files at row-group
# boundaries, hence 1K-row groups); production targets ~128 MB files.
DeltaTable(BRONZE).optimize.z_order(["tenant_id"], target_size=64 * 1024,
                                    writer_properties=WriterProperties(max_row_group_size=1_000))

# %% [markdown]
# ## 3. Proof 1 — scan every Parquet file on disk (not just the live log)

# %%
def pii_hits_on_disk(table_path: str) -> int:
    hits = 0
    for f in Path(table_path).rglob("*.parquet"):
        if "_delta_log" in f.parts:
            continue
        t = pq.read_table(f, columns=["prompt", "response"])
        text = "\n".join(t.column("prompt").to_pylist() + t.column("response").to_pylist())
        hits += sum(1 for v in planted if v in text)
    return hits


files_on_disk = sum(1 for f in Path(BRONZE).rglob("*.parquet") if "_delta_log" not in f.parts)
print(f"parquet files scanned: {files_on_disk}   raw PII values found: {pii_hits_on_disk(BRONZE)}")

t = DeltaTable(BRONZE).to_pyarrow_table(filters=[("tenant_id", "=", "t007")])
email_tokens = {m for p in t.column("prompt").to_pylist() for m in re.findall(r"<EMAIL:\w+>", p)}
same = tokenize(f"from {emails[0]}", "t007")[5:] == tokenize(f"to   {emails[0]}", "t007")[5:]
cross = tokenize(emails[0], "t007") != tokenize(emails[0], "t008")
print(f"t007: {len(email_tokens)} distinct email tokens; deterministic in tenant={same}; "
      f"unlinkable across tenants={cross}")

# %% [markdown]
# ## 4. Gold before retention: daily aggregates per tenant (kept 1 year)

# %%
write_deltalake(GOLD, bronze.group_by(["event_date", "tenant_id"]).aggregate(
    [("request_id", "count"), ("cost_usd", "sum"), ("latency_ms", "max")]), mode="overwrite")

aa = pa.table(DeltaTable(BRONZE).get_add_actions(flatten=True)).to_pylist()
touched = sum(1 for a in aa if a["min.tenant_id"] <= "t007" <= a["max.tenant_id"])
print(f"tenant point query: {touched}/{len(aa)} Bronze files touched after Z-order")

# %% [markdown]
# ## 5. Proof 2 — 7-day retention is physical, not just logical

# %%
cutoff = (start + timedelta(days=N_DAYS - 7)).strftime("%Y-%m-%d")   # keep the last 7 UTC days
v_before = DeltaTable(BRONZE).version()
bytes_before = sum(f.stat().st_size for f in Path(BRONZE).rglob("*.parquet"))

m = DeltaTable(BRONZE).delete(f"event_date < '{cutoff}'")   # partition-aligned → whole files dropped
print(f"cutoff={cutoff}: files removed={m['num_removed_files']}, rows copied={m.get('num_copied_rows', 0)}")
print(f"before VACUUM, time travel to v{v_before} still returns "
      f"{DeltaTable(BRONZE, version=v_before).count():,} rows → deleted ≠ gone")

DeltaTable(BRONZE).vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
bytes_after = sum(f.stat().st_size for f in Path(BRONZE).rglob("*.parquet"))
old_dirs = [p.name for p in Path(BRONZE).glob("event_date=*")
            if p.name.split("=")[1] < cutoff and any(p.glob("*.parquet"))]
try:
    DeltaTable(BRONZE, version=v_before).to_pyarrow_table()
    tt = "STILL READABLE"
except Exception as e:  # noqa: BLE001
    tt = f"fails ({type(e).__name__})"

dates = sorted(set(DeltaTable(BRONZE).to_pyarrow_table().column("event_date").to_pylist()))
gold_days = len(set(DeltaTable(GOLD).to_pyarrow_table().column("event_date").to_pylist()))
print(f"after VACUUM: bytes {bytes_before:,} → {bytes_after:,}; old partitions with files: {old_dirs}")
print(f"time travel to v{v_before}: {tt}")
print(f"Bronze days kept: {len(dates)} ({dates[0]} … {dates[-1]});  Gold days kept: {gold_days}")

# %%
checks = {
    "no raw PII in any parquet file": pii_hits_on_disk(BRONZE) == 0,
    "tokens deterministic within tenant": same,
    "tokens unlinkable across tenants": cross,
    "tenant query touches ≤ 25% of files": touched <= len(aa) // 4,
    "Bronze keeps exactly 7 days": len(dates) == 7 and dates[0] == cutoff,
    "old payload files physically gone": not old_dirs and bytes_after < bytes_before,
    "pre-delete version unreadable": tt.startswith("fails"),
    "Gold keeps all 10 days": gold_days == N_DAYS,
}
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(checks.values())
print(f"\nPoC complete. (scratch dir: {ROOT})")
