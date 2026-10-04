# Khai báo sử dụng AI

**Công cụ:** Claude Code (model Claude Opus 5.5), chạy trong VS Code trên máy cá nhân.

## AI đã hỗ trợ những gì

- Đọc tài liệu lab (README, RUBRIC, CHECKPOINTS, SUBMISSION, RULES) và lập danh sách việc cần làm.
- Tạo venv, cài dependencies, chạy smoke test, sinh dữ liệu, chạy `pytest` và `run_all.py` trên máy này.
- Chuyển 8 notebook sang `.ipynb` và thực thi trong Jupyter kernel để giữ output.
- Thêm các cell bằng chứng (NB1 `_delta_log/` + cờ schema enforcement thật, NB3 số dòng theo version,
  NB4 kiểm tra chất lượng Gold, NB6 đối chiếu checkpoint) — chi tiết trong [INFO.md](INFO.md).
- Viết nháp các mục "Giải thích kết quả" trong notebook, `INFO.md` và `REFLECTION.md`.
- Tạo screenshot bằng cách render các cell đã chạy (nbconvert) và chụp bằng Edge headless.
- Bonus: viết nháp `bonus/ARCHITECTURE.md` (chọn topic, quyết định, failure modes, phép tính chi phí)
  và viết + chạy PoC `bonus/poc/poc_pii_retention.py`; số liệu PoC trong tài liệu lấy từ lần chạy thật.
  Giá cloud là list price công khai được dùng làm giả định, cần kiểm tra lại khi triển khai.
- Phân tích một số chi tiết trong output: Gold có 8 ngày do `CAST(ts AS DATE)` dùng múi giờ UTC+7;
  dòng "5 files" ở NB6 gồm 2 checkpoint trong `_delta_log/`; dry-run VACUUM báo "0 B" vì trả về
  đường dẫn tương đối.

## Những gì AI không làm

- Không tạo hay sửa số liệu: mọi con số trong notebook, screenshot và phần giải thích đều lấy từ
  các lần chạy thật trên máy này.
- Không hạ ngưỡng, không bỏ assertion; cờ hardcode duy nhất (NB1) được thay bằng kiểm tra thật.

Người nộp chịu trách nhiệm đọc lại, chạy lại và giải thích được toàn bộ mã nguồn cùng kết quả.
