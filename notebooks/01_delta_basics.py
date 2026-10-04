# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB1 — Delta Lake Basics (lightweight path)
#
# **Stack:** `deltalake` (delta-rs) + Polars + DuckDB. No Spark, no JVM.
# Maps to slide §2 (Delta Lake) + deliverable bullet 1.
#
# > Spark equivalent: `spark.read.format("delta").load(path)` ↔ `DeltaTable(path).to_pyarrow_table()`.
# > Same on-disk format, different binding.

# %%
import _setup  # noqa: F401  -- adds scripts/ to sys.path (file-relative)
import polars as pl
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

table_path = path("scratch", "users_delta")
reset(table_path)  # idempotent rerun

# %% [markdown]
# ## 1. Write a Delta table

# %%
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "age": [30, 25, 35],
    "city": ["Hanoi", "HCMC", "Danang"],
})
write_deltalake(table_path, df.to_arrow(), mode="overwrite")

# %% [markdown]
# ## 2. Read it back + inspect transaction log
#
# Look at `_lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json` —
# that's the transaction log. Same JSON format Spark/Databricks would write.

# %%
dt = DeltaTable(table_path)
print(pl.from_arrow(dt.to_pyarrow_table()))
print("\nHistory:")
for h in dt.history():
    print(f"  v{h['version']}  {h['operation']}  {h.get('operationMetrics', {})}")

# %% [markdown]
# ## 3. Schema enforcement — try to write a wrong schema

# %%
bad = pl.DataFrame({"id": [4], "name": ["dan"], "age": ["thirty"], "city": ["Hue"]})
version_before_bad = DeltaTable(table_path).version()
try:
    write_deltalake(table_path, bad.to_arrow(), mode="append")
    schema_blocked = False
    print("UNEXPECTED: bad write succeeded — schema enforcement broken")
except Exception as e:
    schema_blocked = True
    msg = str(e).splitlines()[0][:120]
    print(f"BLOCKED by schema enforcement (expected): {type(e).__name__}: {msg}")
# Evidence beyond the message: a blocked write must not create a new commit.
print(f"Table version before/after bad write: {version_before_bad} → {DeltaTable(table_path).version()}")

# %% [markdown]
# ## 4. Schema evolution (opt-in)

# %%
new = pl.DataFrame({
    "id": [4], "name": ["dan"], "age": [28], "city": ["Hue"], "tier": ["premium"],
})
write_deltalake(table_path, new.to_arrow(), mode="append", schema_mode="merge")
dt = DeltaTable(table_path)
# Sort by id so the printout is stable across reruns — Delta does not
# preserve write-order across appends.
print(pl.from_arrow(dt.to_pyarrow_table()).sort("id"))

# %% [markdown]
# ## 5. Query with DuckDB via Arrow (part of the required notebook)

# %%
import duckdb

# We hand DuckDB an Arrow table rather than calling `delta_scan()`. delta_scan
# autoloads a DuckDB extension over the network — fine at home, a support
# ticket in a firewalled classroom. Arrow registration is zero-copy and offline.
con = duckdb.connect()
con.register("users", DeltaTable(table_path).to_pyarrow_table())
tier_counts = con.sql("SELECT tier, count(*) AS n FROM users GROUP BY 1 ORDER BY 1").fetchall()
print(tier_counts)

# %% [markdown]
# ## 6. Evidence — the `_delta_log/` on disk and one commit JSON
#
# Each line of a commit file is one action (`commitInfo`, `protocol`,
# `metaData`, `add`, …). Version 1 is the schema-merge append, so its
# `metaData` action carries the new schema with `tier`.

# %%
import json as _json  # noqa: E402
from pathlib import Path as _P  # noqa: E402

_log_dir = _P(table_path) / "_delta_log"
print(f"{_log_dir}:")
for f in sorted(_log_dir.iterdir()):
    print(f"  {f.name:<28} {f.stat().st_size:>6} B")

_commit = _log_dir / "00000000000000000001.json"
print(f"\n--- {_commit.name} ---")
for line in _commit.read_text().splitlines():
    action = _json.loads(line)
    kind = next(iter(action))
    body = action[kind]
    if kind == "metaData":
        fields = [(f["name"], f["type"]) for f in _json.loads(body["schemaString"])["fields"]]
        print(f"[{kind}] schema fields = {fields}")
    elif kind == "add":
        print(f"[{kind}] path={body['path'][:48]}…  size={body['size']} B  stats={body.get('stats')}")
    else:
        print(f"[{kind}] {_json.dumps(body)[:160]}")

# %% [markdown]
# ### Giải thích kết quả NB1
#
# * **Transaction log:** `_delta_log/` có 2 commit JSON (`…000.json` = WRITE tạo bảng 3 dòng,
#   `…001.json` = append có `schema_mode="merge"`). Lần ghi sai kiểu **không** tạo commit nào —
#   version đứng yên ở 0 trước và sau lệnh ghi lỗi. Đây chính là tính nguyên tử: ghi hỏng thì
#   log không thay đổi, reader không bao giờ thấy trạng thái nửa vời.
# * **Schema enforcement:** ghi `age="thirty"` vào cột `Int64` bị chặn với lỗi
#   `Cast error: Cannot cast string 'thirty' to value of Int64 type`. Delta so dữ liệu mới với
#   schema đã lưu trong action `metaData` của log, nên dữ liệu bẩn bị chặn ngay tại lúc ghi
#   thay vì làm hỏng các job đọc phía sau.
# * **Schema evolution là opt-in:** chỉ khi truyền `schema_mode="merge"` thì cột `tier` mới được
#   thêm; commit `…001.json` chứa action `metaData` với schema 5 cột. 3 dòng cũ đọc ra `tier = null`
#   (không cần backfill), nên DuckDB thấy đúng 2 nhóm: `premium` (1) và `NULL` (3).
# * Ghi chú: trong bản gốc cờ "schema enforcement blocked" bị hardcode `True`; ở đây thay bằng
#   biến `schema_blocked` được gán từ kết quả thật của lệnh ghi lỗi, tiêu chí giữ nguyên.

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] `_delta_log/` contains JSON files
# - [ ] Schema enforcement blocked the bad write
# - [ ] schema_mode="merge" added the `tier` column
# - [ ] DuckDB query returned 2 tier groups
# The final schema-enforcement flag is hardcoded; inspect the actual error
# from the bad-write cell rather than treating that PASS line as proof.

# %%
from pathlib import Path as _Path  # noqa: E402

_log = sorted(_Path(table_path).glob("_delta_log/*.json"))
_cols = DeltaTable(table_path).schema().to_arrow().names
checks = {
    "_delta_log/ has JSON commits": len(_log) >= 2,
    # Was a hardcoded True placeholder; now set by the bad-write cell above.
    "schema enforcement blocked bad write": schema_blocked and DeltaTable(table_path).version() == 1,
    "tier column added via schema_mode=merge": "tier" in _cols,
    "duckdb sees 2 tier groups": len(tier_counts) == 2,
}
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(checks.values()), "NB1 incomplete — see FAIL rows above"
print("\nNB1 complete.")
