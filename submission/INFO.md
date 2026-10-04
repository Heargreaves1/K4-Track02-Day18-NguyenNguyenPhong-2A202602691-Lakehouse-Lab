# Thông tin bài nộp

| Mục | Giá trị |
|---|---|
| Họ tên | Nguyen Nguyen Phong |
| MSSV | 2A202602691 |
| Mã bài | K4-Track02-Day18 — Lakehouse Lab |
| Repo | `K4-Track02-Day18-NguyenNguyenPhong-2A202602691-Lakehouse-Lab` |
| Đường chạy | **Lightweight** cho cả 8 notebook (NB1–NB4 **không** dùng Spark) |
| Python | 3.11.9 (venv `.venv`) |
| Hệ điều hành | Windows 11 Home 10.0.26200, PowerShell |
| Ngày chạy | 04/10/2026 |

## Phiên bản thư viện chính

`deltalake 1.6.6` · `pyiceberg 0.12.0` (`pyiceberg-core 0.10.1`) · `duckdb 1.5.6` · `polars 1.44.2` ·
`pyarrow 25.0.1` · `numpy 2.4.6` · `jupyterlab 4.6.4` · `jupytext 1.19.5` · `pytest 9.1.1`

## Kết quả kiểm tra (PowerShell, từ thư mục gốc repo)

| Lệnh | Kết quả |
|---|---|
| `.\.venv\Scripts\python.exe scripts/verify_lite.py` | 9/9 checks PASS |
| `.\.venv\Scripts\python.exe scripts/generate_data_lite.py` | Bronze 200 000 dòng (9 948 bản trùng) |
| `.\.venv\Scripts\python.exe scripts/generate_ai_data.py` | 2 000 docs, 200 blobs, 1 578 trajectory steps |
| `.\.venv\Scripts\python.exe -m pytest` | **24 passed** |
| `.\.venv\Scripts\python.exe scripts/run_all.py` | **8/8 passed** (chạy lại sau khi thêm cell) |

## Bài nộp

- `notebooks/` — 8 notebook `.ipynb` đã thực thi trong Jupyter kernel, giữ output; mỗi notebook có
  mục **"Giải thích kết quả"** đọc lại các con số đo được.
- `screenshots/` — 15 ảnh, ít nhất 1 ảnh/notebook, chụp các cell code + output thật từ các notebook trên.
- `REFLECTION.md` — reflection (≤ 200 từ); `AI_USAGE.md` — khai báo phạm vi dùng AI.
- `bonus/ARCHITECTURE.md` — architecture brief, Topic A (LLM observability 1B req/ngày);
  `bonus/poc/poc_pii_retention.py` (+ bản `.ipynb` đã chạy) — PoC tokenize PII lúc landing + retention
  7 ngày xóa vật lý. Chạy từ gốc repo: `.\.venv\Scripts\python.exe submission/bonus/poc/poc_pii_retention.py`
  → 8/8 check PASS.

## Thay đổi so với notebook gốc

Chỉ **thêm** cell bằng chứng/giải thích, không hạ ngưỡng hay bỏ assertion nào:

| Notebook | Thay đổi | Lý do |
|---|---|---|
| NB1 | Cờ "schema enforcement blocked" trước đây hardcode `True` → giờ lấy từ kết quả thật của lần ghi lỗi (`schema_blocked`) và kiểm tra version không tăng; thêm cell liệt kê `_delta_log/` và in nội dung commit `…001.json` | Rubric yêu cầu bằng chứng `_delta_log/` + commit JSON; cờ hardcode không chứng minh được gì |
| NB3 | In thêm số dòng ở v2, số dòng `score<0` ở v3 và sau RESTORE | Cho thấy dữ liệu lỗi tồn tại trước khi rollback và vẫn còn trong lịch sử |
| NB4 | Thêm cell kiểm tra Gold: p50 ≤ p95, `cost_usd > 0`, `error_rate ∈ [0,1]`, ngày nào cũng đủ 3 model, 3 tầng có `_delta_log` | Notebook gốc chưa assert các yêu cầu này (CHECKPOINTS.md) |
| NB6 | Thêm cell đối chiếu checkpoint trong `_delta_log/` và số data file trên đĩa vs trong log | Giải thích vì sao output gốc báo "5 files" dù chỉ có 3 orphan |
| NB1–NB8 | Thêm markdown "Giải thích kết quả" | Rubric yêu cầu con số **và** cách đọc nó |
