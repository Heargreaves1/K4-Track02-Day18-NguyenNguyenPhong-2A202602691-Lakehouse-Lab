# Reflection — Lakehouse Anti-Pattern

**Anti-pattern:** small files + không chạy maintenance (ghi streaming theo micro-batch nhưng
không có job compaction, expiry và dọn orphan).

**Vì sao hệ thống mình quan tâm dễ gặp:** mình quan tâm tới log của ứng dụng LLM/chatbot. Mỗi
request sinh một bản ghi nhỏ, được đẩy liên tục theo micro-batch vài giây để dashboard gần
real-time. Từng commit đều đúng, nhưng tích lũy thành rất nhiều file vài chục KB. Lab đo đúng hiện
tượng này: NB6 có 200 commit → 200 file ~51 KB; ở NB5, metadata còn lớn gấp ~2.9 lần data khi file
quá nhỏ. Chi phí còn bị che giấu: VACUUM của `deltalake` không thấy 3 orphan từ writer crash, và
PyIceberg expire 20 → 3 snapshot nhưng không xóa file nào.

**Cách phòng tránh:**
1. Sửa ở writer trước: tăng trigger interval hoặc gom batch.
2. Lên lịch compaction + Z-order theo cột lọc chính (NB2: chỉ còn đọc 1/55 file).
3. Chạy expiry và orphan sweep thành cặp, có age guard và retention ≥ 7 ngày.
4. Theo dõi số file và kích thước file trung bình của từng bảng như một metric cảnh báo.

**Sử dụng AI:** xem [AI_USAGE.md](AI_USAGE.md).
