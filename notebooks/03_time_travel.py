# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB3 — Time Travel + MERGE Upsert (lightweight)
#
# Maps to slide §3 + deliverable bullet 3.
#
# > Spark equivalent: `option("versionAsOf", N)` ↔ `DeltaTable(path, version=N)`
# >                   `MERGE INTO ...`           ↔ `dt.merge(source, predicate)`

# %%
import _setup  # noqa: F401  -- adds scripts/ to sys.path
import time
import polars as pl
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

table_path = path("scratch", "customers_tt")
reset(table_path)

# %% [markdown]
# ## 1. Build version history
# v0: initial 100K · v1: schema add · v2: MERGE upsert · v3: bad data

# %%
# v0 — initial load
v0 = pl.DataFrame({
    "customer_id": list(range(100_000)),
    "status":      ["active"] * 100_000,
    "score":       [i % 1000 for i in range(100_000)],
})
write_deltalake(table_path, v0.to_arrow(), mode="overwrite")

# v1 — add `tier` column (schema evolution)
v1 = (pl.from_arrow(DeltaTable(table_path).to_pyarrow_table())
        .with_columns(
            pl.when(pl.col("score") > 800).then(pl.lit("gold")).otherwise(pl.lit("silver")).alias("tier")
        ))
write_deltalake(table_path, v1.to_arrow(), mode="overwrite", schema_mode="overwrite")

# v2 — MERGE upsert 100K (50K updates, 50K inserts)
updates = pl.DataFrame({
    "customer_id": list(range(50_000, 150_000)),
    "status":      ["vip"] * 100_000,
    "score":       [999] * 100_000,
    "tier":        ["platinum"] * 100_000,
})
t0 = time.time()
(DeltaTable(table_path)
    .merge(source=updates.to_arrow(),
           predicate="t.customer_id = s.customer_id",
           source_alias="s", target_alias="t")
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute())
print(f"MERGE 100K rows: {time.time()-t0:.2f}s")

# v3 — simulate bad data
bad = pl.DataFrame({
    "customer_id": list(range(50)),
    "status":      [None] * 50,
    "score":       [-1] * 50,
    "tier":        ["UNKNOWN"] * 50,
}, schema={"customer_id": pl.Int64, "status": pl.Utf8, "score": pl.Int64, "tier": pl.Utf8})
write_deltalake(table_path, bad.to_arrow(), mode="append")

# %% [markdown]
# ## 2. history() — audit trail

# %%
for h in DeltaTable(table_path).history():
    print(f"  v{h['version']:>2}  {h['operation']:<25}  metrics={h.get('operationMetrics', {})}")

# %% [markdown]
# ## 3. Time-travel queries

# %%
v0_count = DeltaTable(table_path, version=0).to_pyarrow_table().num_rows
v1_cols  = DeltaTable(table_path, version=1).schema().to_arrow().names
v2_count = DeltaTable(table_path, version=2).to_pyarrow_table().num_rows
v3_bad   = DeltaTable(table_path, version=3).to_pyarrow_table(filters=[("score", "<", 0)]).num_rows
print(f"v0 row count: {v0_count}")
print(f"v1 schema:    {v1_cols}")
print(f"v2 row count (after MERGE): {v2_count}")
print(f"v3 rows with score<0 (the bad append): {v3_bad}")

# %% [markdown]
# ## 4. RESTORE bad version (rollback)
#
# `restore(2)` rewinds the *current* state of the table to whatever it was
# at version 2, recorded as a new version (v4). The old versions stay in
# history — restore is itself a transaction, fully auditable.

# %%
t0 = time.time()
dt = DeltaTable(table_path)
dt.restore(2)
print(f"RESTORE → v2: {time.time()-t0:.2f}s   (target < 30s)")

# Verify the bad rows are gone — use delta-rs's native filter pushdown.
# (DuckDB's delta extension as of 1.5.x is stricter about post-RESTORE
# protocol entries than delta-rs writes; routing through delta-rs end-to-end
# avoids the InvalidProtocolError race.)
dt_after = DeltaTable(table_path)
bad_count = dt_after.to_pyarrow_table(filters=[("score", "<", 0)]).num_rows
print(f"Rows with score<0 after restore: {bad_count}  (expected 0)")

# %% [markdown]
# ## 5. history() — final audit trail (now includes the RESTORE)

# %%
final_history = DeltaTable(table_path).history()
for h in final_history:
    print(f"  v{h['version']:>2}  {h['operation']:<25}")
print(f"\nTotal versions: {len(final_history)}  (target ≥ 5)")

# The bad rows are gone from the current version but still reachable by time travel.
still_v3 = DeltaTable(table_path, version=3).to_pyarrow_table(filters=[("score", "<", 0)]).num_rows
print(f"score<0 rows: current={bad_count}, still visible at v3={still_v3}")

# %% [markdown]
# ### Giải thích kết quả NB3
#
# * **MERGE 100K dòng thành công:** metrics của v2 ghi `num_source_rows=100000`,
#   `num_target_rows_updated=50000` (id 50 000–99 999 đã có → update thành `vip/platinum`) và
#   `num_target_rows_inserted=50000` (id 100 000–149 999 chưa có → insert). Kết quả 150 000 dòng.
#   MERGE ghi lại file bị ảnh hưởng (1 file removed, 1 file added) trong **một** commit, nên reader
#   chỉ thấy trước-MERGE hoặc sau-MERGE, không bao giờ thấy nửa chừng.
# * **Time travel:** `version=0` đọc lại đúng 100 000 dòng ban đầu; `version=1` cho schema có thêm
#   `tier`; `version=3` thấy 50 dòng lỗi `score=-1`. Mỗi version là một snapshot bất biến được dựng
#   lại từ log, không phải bản sao dữ liệu.
# * **RESTORE:** `restore(2)` không xóa lịch sử mà ghi **commit mới v4 (RESTORE)** trỏ tập file hiện
#   hành về đúng trạng thái v2 → số dòng `score < 0` ở version hiện tại = **0**. History có
#   **5 version** (WRITE, WRITE, MERGE, WRITE, RESTORE), đạt ngưỡng ≥ 5 và có dòng RESTORE.
# * **Hệ quả cần nhớ:** dữ liệu lỗi vẫn đọc được ở v3 cho tới khi VACUUM dọn các file đó — tốt cho
#   audit/điều tra sự cố, nhưng nghĩa là "rollback" không đồng nghĩa "xóa vật lý" (xem NB6, NB8).

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] history() shows ≥ 5 versions (incl. RESTORE itself)
# - [ ] MERGE 100K finished in < 60s (likely < 1s on lightweight path)
# - [ ] RESTORE finished in < 30s and removed bad rows

# %%
ops = [h["operation"] for h in final_history]
checks = {
    "history ≥ 5 versions":          len(final_history) >= 5,
    "history includes the RESTORE":  any("RESTORE" in o.upper() for o in ops),
    "MERGE recorded in history":     any("MERGE" in o.upper() for o in ops),
    "bad rows gone after restore":   bad_count == 0,
}
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(checks.values()), "NB3 incomplete — see FAIL rows above"
print("\nNB3 complete.")
