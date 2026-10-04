# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB4 — Medallion Pipeline (Bronze → Silver → Gold), lightweight
#
# **Use case:** LLM observability — exact schema from slide §8 (Lakehouse cho AI/ML) medallion frame.
# Maps to deliverable bullet 4 (the Milestone-1 Lakehouse artifact).
#
# Pre-req: ran `make data` — but if you jumped straight here, the cell below
# generates the Bronze sample for you rather than failing on a missing path.

# %%
import _setup  # noqa: F401  -- adds scripts/ to sys.path
from pathlib import Path

import polars as pl
import duckdb
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

BRONZE = path("bronze", "llm_calls_raw")
SILVER = path("silver", "llm_calls")
GOLD   = path("gold",   "llm_daily_metrics")

# Self-healing pre-req (same pattern as NB7/NB8). Without this, skipping
# `make data` surfaces as a raw `Os { code: 2, kind: NotFound }` from the Rust
# layer — technically correct, useless to a student.
if not Path(BRONZE).exists():
    print("Bronze not found — running scripts/generate_data_lite.py first ...")
    import generate_data_lite

    generate_data_lite.main()

# %% [markdown]
# ## Bronze — verify raw is loaded

# %%
bronze_n = DeltaTable(BRONZE).to_pyarrow_table().num_rows
print(f"Bronze rows: {bronze_n:,}")
print(pl.from_arrow(DeltaTable(BRONZE).to_pyarrow_table().slice(0, 2)))

# %% [markdown]
# ## Silver — parse, validate, dedup
#
# Rules: drop malformed JSON, dedupe by `request_id`, project typed columns.

# %%
reset(SILVER)

# DuckDB does the JSON parse + dedup in one query — Polars also works,
# DuckDB just has nicer JSON syntax for this case.
# DuckDB reads Delta through Arrow, not through `delta_scan()`. The latter
# autoloads an extension over the network; Arrow registration is offline and
# zero-copy, so the lab works on a locked-down machine.
con = duckdb.connect()
con.register("bronze", DeltaTable(BRONZE).to_pyarrow_table())

silver_arrow = con.sql(f"""
    WITH parsed AS (
      SELECT
        request_id,
        ts,
        CAST(ts AS DATE)                            AS date,
        json_extract_string(raw_json, '$.model')          AS model,
        json_extract_string(raw_json, '$.user_id')        AS user_id,
        CAST(json_extract(raw_json, '$.usage.input')  AS INTEGER) AS prompt_tokens,
        CAST(json_extract(raw_json, '$.usage.output') AS INTEGER) AS completion_tokens,
        CAST(json_extract(raw_json, '$.latency_ms')   AS INTEGER) AS latency_ms,
        json_extract_string(raw_json, '$.status')         AS status,
        ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts) AS rn
      FROM bronze
    )
    SELECT request_id, ts, date, model, user_id,
           prompt_tokens, completion_tokens, latency_ms, status
    FROM parsed
    WHERE rn = 1 AND model IS NOT NULL
""").arrow()

write_deltalake(SILVER, silver_arrow, mode="overwrite", partition_by=["date"])

silver_n = DeltaTable(SILVER).to_pyarrow_table().num_rows
print(f"Silver rows: {silver_n:,}  (Bronze {bronze_n:,} → dedup dropped {bronze_n - silver_n:,})")
assert silver_n < bronze_n, (
    "Silver has the same row count as Bronze — dedup did not run. "
    "Did you regenerate Bronze with the latest generator (which injects retries)?"
)

# %% [markdown]
# ## Gold — aggregate to (date, model) metrics

# %%
reset(GOLD)

# Illustrative cost model — NOT canonical pricing.
# (input USD / 1M tokens, output USD / 1M tokens)
COST_TABLE = """
  VALUES
    ('claude-haiku-4-5',  0.80,  4.00),
    ('claude-sonnet-4-6', 3.00, 15.00),
    ('claude-opus-4-7', 15.00, 75.00)
"""

con.register("silver", DeltaTable(SILVER).to_pyarrow_table())
gold_arrow = con.sql(f"""
    WITH cost(model, c_in, c_out) AS ({COST_TABLE})
    SELECT
      s.date,
      s.model,
      QUANTILE_CONT(s.latency_ms, 0.50) AS p50_latency_ms,
      QUANTILE_CONT(s.latency_ms, 0.95) AS p95_latency_ms,
      SUM(s.prompt_tokens)              AS total_prompt_tokens,
      SUM(s.completion_tokens)          AS total_completion_tokens,
      AVG(CASE WHEN s.status <> 'ok' THEN 1.0 ELSE 0.0 END) AS error_rate,
      (SUM(s.prompt_tokens)     * c.c_in  / 1e6) +
      (SUM(s.completion_tokens) * c.c_out / 1e6) AS cost_usd
    FROM silver s
    JOIN cost c USING (model)
    GROUP BY s.date, s.model, c.c_in, c.c_out
    ORDER BY s.date, s.model
""").arrow()

write_deltalake(GOLD, gold_arrow, mode="overwrite", partition_by=["date"])

# Z-order for fast filter-by-model dashboards
DeltaTable(GOLD).optimize.z_order(["model"])

# %% [markdown]
# ## Verify Gold

# %%
gold_df = pl.from_arrow(DeltaTable(GOLD).to_pyarrow_table())
print(gold_df)

# Slide-5 deliverable: "Gold p50/p95/cost qua ≥ 7 ngày". Make that explicit.
n_dates = gold_df.select("date").n_unique()
n_models = gold_df.select("model").n_unique()
print(
    f"\n──── Gold deliverable metrics ────\n"
    f"  Distinct dates:   {n_dates:>3}   (target ≥ 7)\n"
    f"  Distinct models:  {n_models:>3}\n"
    f"  Total Gold rows:  {gold_df.height:>3}   (= dates × models)"
)
assert n_dates >= 7, (
    f"Gold has only {n_dates} dates — slide deliverable requires ≥ 7. "
    "Re-run `make data` (the generator spreads across 7 UTC days)."
)

# %% [markdown]
# ## Gold quality checks (not asserted by the original notebook)
#
# The rubric asks for *correct* Gold: p50 ≤ p95, positive cost, error_rate in
# [0, 1], every date covered by all 3 models — plus all three layers on disk.

# %%
for layer, p in [("bronze", BRONZE), ("silver", SILVER), ("gold", GOLD)]:
    _dt = DeltaTable(p)
    print(f"{layer:<7} v{_dt.version()}  files={len(_dt.file_uris()):>3}  rows={_dt.count():>7,}  "
          f"_delta_log={Path(p, '_delta_log').exists()}  {p}")

gold_sorted = gold_df.sort(["date", "model"]).with_columns(
    pl.col("error_rate").round(4), pl.col("cost_usd").round(2))
with pl.Config(tbl_rows=-1, tbl_width_chars=200, fmt_str_lengths=20):
    print(gold_sorted.select("date", "model", "p50_latency_ms", "p95_latency_ms",
                             "error_rate", "cost_usd"))

per_date = gold_df.group_by("date").agg(pl.col("model").n_unique().alias("n_models"))
gold_checks = {
    "silver < bronze (dedup)":       silver_n < bronze_n,
    "≥ 7 dates":                     n_dates >= 7,
    "3 models":                      n_models == 3,
    "every date has all 3 models":   per_date["n_models"].min() == 3,
    "p50 ≤ p95 on every row":        bool((gold_df["p50_latency_ms"] <= gold_df["p95_latency_ms"]).all()),
    "cost_usd > 0 on every row":     bool((gold_df["cost_usd"] > 0).all()),
    "error_rate in [0, 1]":          bool(gold_df["error_rate"].is_between(0, 1).all()),
}
for k, v in gold_checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(gold_checks.values()), "Gold quality check failed — see FAIL rows above"

# Why 8 dates from a 7-day generator? CAST(ts AS DATE) on a TIMESTAMPTZ uses the
# DuckDB session time zone, not UTC.
session_tz = con.sql("SELECT current_setting('TimeZone')").fetchone()[0]
print(f"\nDuckDB session TimeZone: {session_tz}")
print("Silver rows per date:", con.sql("SELECT date, count(*) FROM silver GROUP BY 1 ORDER BY 1").fetchall())

# %% [markdown]
# ### Giải thích kết quả NB4
#
# * **Bronze → Silver:** Bronze có 200 000 dòng thô, trong đó generator cố ý chèn 9 948 lần retry
#   trùng `request_id`. Silver dùng `ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts)` và giữ
#   `rn = 1` → còn **190 052** dòng, tức đúng bằng số `request_id` duy nhất. Nếu không dedup, mọi
#   retry sẽ bị tính hai lần vào token và chi phí ở Gold.
# * **Gold** tổng hợp theo `(date, model)`: **8 ngày × 3 model = 24 dòng** (đạt ≥ 7 × 3). Mọi kiểm tra
#   chất lượng PASS: p50 ≤ p95, `cost_usd > 0`, `error_rate ∈ [0, 1]`, ngày nào cũng đủ 3 model.
# * **Đọc số:** latency tăng theo cỡ model (p50 ≈ 0.56 s haiku, ≈ 1.4 s sonnet, ≈ 3.0 s opus; p95 ≈ 2× p50).
#   `error_rate` ≈ 0.05 khớp với tỉ lệ status khác `ok` mà generator gieo (3 % `rate_limited` + 2 % `error`).
#   Chi phí theo giá minh họa: sonnet chiếm 60 % traffic nên tổng chi phí/ngày cao nhất, còn opus chỉ
#   10 % request nhưng chi phí gần bằng sonnet vì đơn giá token cao gấp 5×.
# * **Vì sao 8 ngày chứ không phải 7:** dữ liệu trải đúng 7 ngày UTC (01–07/04), nhưng
#   `CAST(ts AS DATE)` trên `TIMESTAMPTZ` dùng múi giờ phiên DuckDB (máy chạy ở UTC+7). Vì vậy 7 giờ
#   cuối ngày 07/04 UTC rơi sang ngày 08/04 giờ địa phương, và ngày 01/04 chỉ có 17 giờ. Hai ngày
#   đầu/cuối là ngày "thiếu" — dashboard so sánh theo ngày sẽ hiểu sai nếu không chuẩn hóa về UTC
#   (ví dụ `CAST(ts AT TIME ZONE 'UTC' AS DATE)`). Đây là bẫy production thật: cùng pipeline chạy ở
#   máy khác múi giờ sẽ cho Gold khác nhau.

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] All three tables exist under `_lakehouse/{bronze,silver,gold}/`
# - [ ] Silver has fewer rows than Bronze (dedup worked)
# - [ ] Gold spans ≥ 7 dates × 3 models (slide §8 medallion contract)
# - [ ] Cost & error_rate columns populated and non-zero
