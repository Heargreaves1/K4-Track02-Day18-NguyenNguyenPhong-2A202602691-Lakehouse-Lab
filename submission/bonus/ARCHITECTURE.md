# Bonus — LLM Observability ở quy mô 1B requests/ngày

**Nguyen Nguyen Phong · 2A202602691 · K4-Track02-Day18 · Topic A**
PoC: [`poc/poc_pii_retention.py`](poc/poc_pii_retention.py) (bản đã chạy: [`poc_pii_retention.ipynb`](poc/poc_pii_retention.ipynb))

---

## 1. Problem statement

Một team foundation-model API log mọi request/response: **1B req/ngày × ~5 KB = 5 TB/ngày raw**
(trung bình 11 574 req/s ≈ 58 MB/s, giả định peak 3× ≈ 35K req/s ≈ 174 MB/s). Yêu cầu:

1. Dashboard cost & latency **theo tenant**, refresh mỗi **5 phút**.
2. Prompt/response đầy đủ giữ **7 ngày** cho incident review; sau đó chỉ giữ **aggregates 1 năm**.
3. **PII phải được redact trước khi bất kỳ ai đọc** — kể cả data engineer có quyền đọc bucket.
4. Tổng chi phí storage **≤ $5 K/tháng**.

Vì sao khó: giữ payload thô 1 năm tốn ~$39K/tháng (mục 5), nên retention phải là xóa **vật lý**
chứ không chỉ logic. "Trước khi ai đọc" nghĩa là PII không được chạm đĩa, nên che ở tầng view là
không đủ. Ghi streaming liên tục sinh small files, trong khi dashboard 5 phút lại cần đọc nhanh.
Ngoài ra percentile không cộng dồn được, nên aggregate 1 năm không thể chỉ lưu p95.

## 2. Architecture diagram

```text
                    ┌──────────── Unity Catalog — control plane [C5] ─────────────┐
                    │ grants bảng/cột · credential vending ngắn hạn · audit đọc    │
                    └────────────────────────────┬────────────────────────────────┘
 API gateways ──► Kafka (raw, CÓ PII) ──► Tokenizer stream (Spark, trigger 60 s)
 1B req/ngày      24 h · RF3 · zstd        HMAC theo tenant · event_date theo UTC   [C1]
 (chỉ service account đọc được)                    │ 1 micro-batch = 1 commit nguyên tử  [C2]
                                                    ▼
 BRONZE  llm_payload_bronze (Delta) · partition event_date (UTC) · Z-order tenant_id   [C3]
         payload ĐÃ tokenize · giữ 7 ngày · DELETE theo partition + VACUUM 24 h         [C4]
                                                    │ Change Data Feed
                                                    ▼
 SILVER  llm_requests_silver · envelope có kiểu · MERGE dedup theo request_id
         KHÔNG có payload · giữ 7 ngày · schema enforcement + cột _rescued              [C6]
                                                    │ CDF, micro-batch 1 phút
                                                    ▼
 GOLD    tenant_metrics_5m (DDSketch latency, tokens, cost, error) · MERGE upsert · 30 ngày
         └─► tenant_metrics_1h (rollup sketch) · 1 năm · RESTORE khi deploy hỏng        [C7]
                                                    │
                                                    ▼
 Trino/DuckDB ──► dashboard theo tenant (5 phút)        Incident review ──► Bronze (đã tokenize)

 Maintenance: OPTIMIZE+Z-order mỗi giờ · VACUUM hằng ngày · orphan diff hằng tuần ·
              checkpoint tự động                                                        [C4]
 FinOps: chỉ S3 Standard (dữ liệu sống 8.5 ngày < 30 ngày tối thiểu của IA) · budget ở mục 5 [C8]
```

| Tag | Concept Day18 áp dụng vào lựa chọn cụ thể |
|---|---|
| C1 | **Security/PII tại Bronze landing**: tokenize trong stream, PII không bao giờ được ghi vào bảng (D3) |
| C2 | **ACID transaction log**: batch lỗi không bao giờ hiện ra một nửa; replay từ Kafka là idempotent |
| C3 | **Partitioning + Z-order/data skipping** cho hot path "lọc theo tenant" (D4; NB2 đo prune 55×) |
| C4 | **Retention, VACUUM, orphan removal, checkpoint** — 4 job maintenance của NB6 (D5) |
| C5 | **Catalog là control plane**: không engine nào cầm credential S3 dài hạn (D2) |
| C6 | **Schema enforcement/evolution** (NB1) + **CDF** để Silver/Gold tăng dần |
| C7 | **Time travel/RESTORE** (NB3) để rollback Gold; **mergeable sketch** cho aggregate 1 năm (D6) |
| C8 | **FinOps**: phép tính storage/compute ở mục 5 quyết định retention và tiering |

## 3. Các quyết định chính và alternatives đã loại

Giả định giá (list price AWS us-east-1, cần kiểm tra lại khi triển khai): S3 Standard
$23/TB-tháng (50 TB đầu), $22 (450 TB tiếp), $21 (sau đó); S3 Standard-IA $12.5/TB-tháng, **tối thiểu
30 ngày**; EBS gp3 $80/TB-tháng; PUT $0.005/1K, GET $0.0004/1K; m6i.2xlarge $0.384/h. Tỉ lệ nén zstd
trên payload JSON giả định **4×**, sẽ đo lại trong MVP. Độ nhạy khi chỉ nén được 2× xem mục 5.

**D1 — Table format: chọn Delta Lake.**
- *Loại Iceberg:* Iceberg có hidden partitioning, giúp tránh bug `event_date` tự suy ra (NB4/NB5), và
  trung lập engine hơn. Nhưng với 1 440 commit/ngày, mỗi commit Iceberg sinh thêm metadata.json + manifest
  list + manifest. NB5 đo được metadata = 290 % data khi commit nhỏ, và NB6 cho thấy expiry của PyIceberg
  không xóa file, phải tự chạy thêm sweep. Delta tự checkpoint mỗi 100 commit (NB6 thấy v99, v199) và có
  CDF dùng sẵn cho Silver→Gold. Mình sẽ đổi sang Iceberg nếu ≥ 2 engine ngoài Spark cần **ghi**.
- *Loại JSON thô trên S3 + Athena:* không có ACID (reader thấy batch ghi dở), không DELETE được theo
  hàng, không có stats để skip file. Retention 7 ngày khi đó chỉ còn là lifecycle rule theo prefix.
- *Loại ClickHouse làm hệ lưu trữ chính:* serve dashboard rất nhanh, nhưng storage gắn với compute trên
  EBS (10.6 TB × RF2 × $80 = $1 700/tháng, gấp ~7 lần S3) và không phải system-of-record mở. Nếu cần,
  có thể thêm ClickHouse sau làm cache cho Gold.

**D2 — Catalog và quyền truy cập: chọn Unity Catalog** (managed hoặc OSS), dùng grants theo
bảng/cột, cấp credential ngắn hạn theo bảng và ghi audit log mỗi lần đọc.
- *Loại Hive Metastore:* quyền dựa trên đường dẫn. Ai có `s3:GetObject` trên bucket là đọc được Bronze,
  không có audit theo bảng và không cấp credential theo bảng được.
- *Loại AWS Glue chỉ dùng IAM:* rẻ và native trên AWS, nhưng quyền vẫn theo prefix S3. Muốn phân quyền
  theo cột thì phải cấu hình thêm Lake Formation cho từng engine, và bị khóa vào AWS.
  Rủi ro cần kiểm tra trong MVP: tính năng cấp credential của bản OSS với engine Trino.

**D3 — PII: chọn tokenize bằng HMAC-SHA256 theo khóa riêng từng tenant, ngay trong stream, trước
khi ghi Bronze.** Khóa tenant = HMAC(master key trong KMS, tenant_id). Token tất định trong cùng
tenant, nên incident review vẫn join được ("cùng một email gặp lỗi 50 lần"), nhưng không link được
giữa các tenant và không đảo ngược được nếu không có khóa.
- *Loại che PII lúc đọc (view hoặc column mask):* PII thô vẫn nằm trên đĩa, trong file cũ của time
  travel và trong backup. Ai có quyền đọc S3 trực tiếp vẫn thấy, nên vi phạm yêu cầu 3.
- *Loại tokenize đảo ngược được (vault hoặc format-preserving encryption):* vault phải chịu được
  35K req/s × vài thực thể PII mỗi request và thành điểm lỗi đơn; với FPE, lộ một khóa là lộ toàn bộ
  lịch sử. Yêu cầu chỉ cần *redact*, không cần detokenize.
- *Loại thay bằng `***`:* rẻ nhất, nhưng mất khả năng join khi điều tra sự cố.
  Giới hạn: regex chỉ bắt PII có cấu trúc (email, SĐT, CCCD, API key). Tên và địa chỉ tự do cần NER, ghi
  thành rủi ro mở (F1).

**D4 — Layout: partition theo `event_date` tính theo UTC, Z-order theo `tenant_id` bên trong partition.**
- *Loại partition theo tenant:* 10K tenant × 7 ngày = 70K partition, phân bố lệch (vài tenant chiếm
  phần lớn traffic) → hàng triệu file nhỏ, và retention không còn là "xóa một partition".
- *Loại liquid clustering hoặc partition theo giờ:* liquid clustering không cho dùng chung với
  partition. Retention theo ngày khi đó phải rewrite các file trộn nhiều ngày thay vì bỏ nguyên file.
  PoC đo được DELETE theo partition: **30 file bị gỡ, 0 dòng phải copy**. Partition theo giờ làm file
  nhỏ đi 24 lần mà retention không cần độ mịn đó.
- *Bắt buộc UTC:* NB4 cho thấy `CAST(ts AS DATE)` theo múi giờ phiên (UTC+7) tạo ra ngày "thiếu" và đẩy
  7 giờ dữ liệu sang ngày khác. Nếu sai ở đây, retention sẽ xóa nhầm 7 giờ. Writer tự tính `event_date`
  theo UTC, có test kiểm tra biên.

**D5 — Retention và lifecycle: DELETE theo partition + VACUUM (retention 24 h) hằng ngày, chỉ dùng S3 Standard.**
- *Loại S3 lifecycle rule xóa theo tuổi object:* rule xóa file ngoài tầm transaction log. Bảng vẫn tham
  chiếu file đã mất → query lỗi `FileNotFound`, và time travel báo dối.
- *Loại chuyển sang IA/Glacier:* IA tính tối thiểu 30 ngày, trong khi dữ liệu chỉ sống 8.5 ngày. Mỗi TB
  ghi vào tốn $12.5 ở IA so với $23 × 8.5/30 = $6.5 ở Standard, tức IA **đắt hơn ~1.9×**. Glacier còn
  thêm phí truy xuất, trong khi incident review cần đọc ngay.
- *Loại giữ payload lâu hơn để "phòng khi cần":* compressed 1 năm = 456 TB ≈ $10K/tháng, gấp đôi cap.
  VACUUM là bắt buộc: PoC chứng minh **deleted ≠ gone**. Trước VACUUM, time travel vẫn trả đủ 100 000
  dòng; sau VACUUM thì đọc lỗi.

**D6 — Gold: bucket 5 phút lưu DDSketch (latency) + tổng token/cost/lỗi, cập nhật bằng MERGE mỗi phút;
sau 30 ngày rollup thành bucket 1 giờ, giữ 1 năm.**
- *Loại lưu thẳng p50/p95:* percentile không cộng dồn được (trung bình các p95 khác p95 của tổng), nên
  không tính được p95 theo tháng.
- *Loại tính lại percentile chính xác từ Silver:* Silver chỉ giữ 7 ngày theo yêu cầu 2, và mỗi lần tính
  lại phải quét 1B dòng/ngày. DDSketch merge được và sai số tương đối có giới hạn (cấu hình 1 %), nên
  MERGE cộng dồn được cả sự kiện đến muộn vào đúng bucket.

**D7 — Nhịp ingest và nén: micro-batch 60 s, optimized write, zstd level 3.**
- *Loại trigger 5 s:* 17 280 commit/ngày; NB6 cho thấy chính sự tích lũy commit là lỗi (200 commit →
  200 file 51 KB).
- *Loại batch theo giờ:* trễ hơn yêu cầu 5 phút.
- *Loại snappy:* nén kém hơn, chỉ ~2× trên text. Kafka + Bronze sẽ tăng ~$650/tháng (mục 5).
- *Loại zstd mức 19:* nén chậm hơn khoảng một bậc, cần thêm node ingest đắt hơn phần storage tiết kiệm.

## 4. Failure modes (kịch bản lúc 3 giờ sáng)

**F1 — Tokenizer bỏ sót một định dạng PII mới (ví dụ tiền tố API key mới) → PII thô vào Bronze.**
*Liên quan Day18: time travel, VACUUM, deletion vectors.*
- *Phát hiện:* mỗi phút bơm 1 request canary chứa PII giả đã biết ở mỗi region. Mỗi giờ, scanner (hàm
  `pii_hits_on_disk` của PoC) quét file mới và mẫu 0.1 % payload bằng DLP. Bất kỳ hit nào cũng page
  on-call ngay.
- *Xử lý:* (1) dừng stream; Kafka giữ 24 h nên không mất dữ liệu. (2) Hotfix regex và thêm test.
  (3) Dùng `DESCRIBE HISTORY` khoanh vùng các version bị ảnh hưởng. (4) MERGE payload đã tokenize lại
  (hoặc DELETE rồi replay từ Kafka nếu còn trong 24 h). (5) `REORG TABLE … APPLY (PURGE)` nếu bật
  deletion vectors, rồi `VACUUM RETAIN 0 HOURS` theo quy trình break-glass có phê duyệt — nếu không, file
  cũ vẫn đọc được qua time travel trong cửa sổ retention. Bucket Bronze **tắt S3 versioning**, vì nếu
  không, VACUUM chỉ tạo noncurrent version và PII vẫn còn.

**F2 — Upstream đổi schema (`usage.output` từ int thành object) → job Silver crash, Gold đứng.**
- *Phát hiện:* lag của Gold watermark > 10 phút, stream báo lỗi, consumer lag tăng.
- *Xử lý:* Bronze vẫn chạy vì payload lưu dạng string, schema enforcement chỉ áp lên envelope (NB1).
  Silver parse theo schema tường minh, field lạ vào cột `_rescued`; evolution là opt-in có review. Sau
  khi deploy fix, replay Silver từ CDF của Bronze tính từ version cuối đã commit. MERGE theo
  `request_id` nên replay không nhân đôi dòng.

**F3 — Deploy bảng giá sai → `cost_usd` trên Gold sai gấp 10 lần.**
- *Phát hiện:* đối soát hằng ngày tổng cost trên Gold với hệ thống billing (lệch quá ±2 % là alert),
  cộng anomaly theo tenant (> 3σ).
- *Xử lý:* `RESTORE TABLE gold TO VERSION AS OF <trước deploy>` (NB3: RESTORE là commit mới, lịch sử còn
  nguyên). Sửa bảng giá (là bảng Delta có version) rồi tính lại từ Silver. Ràng buộc: chỉ tính lại được
  trong **7 ngày** Silver, nên đối soát phải chạy hằng ngày chứ không phải hằng tháng.

**F4 — Job maintenance chết âm thầm (VACUUM/OPTIMIZE lỗi credential) → vi phạm retention.**
- *Phát hiện:* metric "partition Bronze cũ nhất còn file trên S3", lấy từ **S3 listing chứ không từ log**
  (NB6: log không thấy orphan). Alert nếu > 8 ngày, hoặc bytes trên S3 / bytes bảng tham chiếu > 1.2,
  hoặc số file/partition vượt ngưỡng.
- *Xử lý:* các job đều idempotent nên chỉ cần chạy lại. Thêm job orphan diff (file trên đĩa − file log
  tham chiếu, có age guard 24 h) để dọn file của writer crash.

## 5. Ước lượng chi phí (back-of-envelope)

**Storage — mục bị cap $5K/tháng (trạng thái ổn định):**

| Thành phần | Phép tính | Dung lượng | $/tháng |
|---|---|---:|---:|
| Kafka buffer (PII thô, 24 h, RF3) | 5 TB ÷ 4 (zstd) × 3 = 3.75 TB, cấp 5 TB gp3 × $80 | 5 TB | 400 |
| Bronze payload (đã tokenize) | 1.25 TB/ngày × (7 + 1 ngày VACUUM + 0.5 compaction) × $23 | 10.6 TB | 244 |
| Silver envelope | 1B × ~40 B nén = 40 GB/ngày × 8 ngày × $23 | 0.32 TB | 7 |
| Gold 5 phút | 288 bucket × 30K tổ hợp × 300 B = 2.6 GB/ngày × 30 ngày | 78 GB | 2 |
| Gold 1 giờ | 2.6 GB ÷ 12 = 0.22 GB/ngày × 365 | 79 GB | 2 |
| S3 PUT | ~160K part/ngày (compaction 128 MB file × 16 part) + commit ≈ 5M/tháng × $0.005/1K | | 25 |
| S3 GET | dashboard 57 600 lần refresh/ngày × 20 file + 1 000 query incident × 200 file ≈ 41M/tháng | | 16 |
| **Tổng** | | **~16 TB** | **≈ $696** |

→ Dùng **~14 % cap**. Độ nhạy: nếu zstd chỉ nén được 2×, Kafka lên $800 và Bronze lên $488, tổng
≈ $1.34K, vẫn dưới cap. **Điều thực sự giữ được budget là retention**, không phải nén:

| Phương án ngây thơ | Phép tính (giá theo bậc) | $/tháng |
|---|---|---:|
| Giữ JSON thô 1 năm | 1 825 TB: 50×23 + 450×22 + 1 325×21 | **38 875** |
| Giữ payload đã nén 1 năm | 456 TB: 50×23 + 406×22 | **10 082** |
| Bronze sang IA sau ngày 1 | 37.5 TB ghi/tháng: 37.5×23/30 + 37.5×12.5 (tối thiểu 30 ngày) | 498 (so với 244) |
| Query tenant không có Z-order | 1 000 query × ~83K file (10.6 TB ÷ 128 MB) × 30 × $0.0004/1K | +1 000 tiền GET (chưa tính compute quét) |

**Compute (không nằm trong cap storage, nhưng phải báo):**

| Thành phần | Phép tính | $/tháng |
|---|---|---:|
| Ingest + tokenize + ghi Bronze | 3 × m6i.2xlarge × $0.384 × 730 h | 841 |
| Silver dedup + Gold sketch MERGE | 1 × m6i.2xlarge 24/7 | 280 |
| Maintenance (OPTIMIZE mỗi giờ, VACUUM, orphan diff) | 4 × m6i.2xlarge × 2 h/ngày × 30 × $0.384 | 92 |
| Serving (Trino/DuckDB trên Gold) | 2 × m6i.xlarge × $0.192 × 730 | 280 |
| Kafka brokers | 3 × m6i.large × $0.096 × 730 | 210 |
| **Tổng compute** | | **≈ $1 703** |

Kiểm tra năng lực tokenizer: peak 174 MB/s ÷ 24 vCPU = **7.3 MB/s/vCPU** cần đạt. Regex 3 mẫu + parse
JSON + HMAC trên JVM thường đạt vài chục MB/s/core, nhưng đây là giả định mà MVP phải đo (AC6).
**Tổng storage + compute ≈ $2.4K/tháng.**

## 6. MVP một tuần

**Slice:** 1 region, replay **1 % traffic** (10M req/ngày ≈ 116 req/s) qua đúng đường Kafka → tokenizer
→ Bronze → Silver → Gold → **một dashboard** "cost và p95 latency theo tenant, 24 h gần nhất". Thêm
15 phút load test ở 35K req/s để đo riêng tokenizer.

| Ngày | Việc |
|---|---|
| 1 | Thư viện tokenizer + bộ test PII (email, SĐT VN, CCCD 12 số, API key) + canary generator |
| 2 | Stream Bronze: `event_date` theo UTC, optimized write, job OPTIMIZE Z-order theo giờ |
| 3 | Silver MERGE dedup + Gold DDSketch MERGE 5 phút + dashboard |
| 4 | Job retention (DELETE partition + VACUUM) + scanner S3 listing + orphan diff |
| 5 | Load test, đo tỉ lệ nén thật, cập nhật bảng chi phí |
| 6–7 | Diễn tập F1 (purge PII) và F3 (RESTORE Gold) có bấm giờ |

**Tiêu chí nghiệm thu:**

| # | Tiêu chí | Cách kiểm tra |
|---|---|---|
| AC1 | 0 hit trên 10K giá trị PII canary trong **mọi** file Parquet của Bronze/Silver, kể cả file đã tombstone | quét toàn bộ S3 listing (mở rộng `pii_hits_on_disk` của PoC) |
| AC2 | p95 (thời điểm hiện trên Gold − ts request) ≤ 5 phút trong 24 h | so watermark Gold với ts |
| AC3 | Sau job retention: không còn file nào có `event_date` < hôm nay − 7 trên S3; time travel về version trước delete báo lỗi; Gold còn đủ | S3 listing + đọc thử version cũ |
| AC4 | Tỉ lệ nén đo được ≥ 3× (nếu thấp hơn thì tính lại mục 5) | bytes Parquet ÷ bytes raw |
| AC5 | Query 1 tenant × 1 ngày đọc ≤ 10 % số file của ngày đó | đếm file từ stats min/max trong log |
| AC6 | Tokenizer đạt ≥ 7.3 MB/s/vCPU ở peak | load test |
| AC7 | Diễn tập F1 hoàn tất < 2 h, file cũ chứa PII đã bị VACUUM | bấm giờ diễn tập |

**Cơ chế khó nhất đã được kiểm chứng bằng PoC** (`poc_pii_retention.py`, chạy bằng venv của lab,
không cần mạng). Đó là kết hợp *PII không chạm đĩa* với *retention là xóa vật lý*:

| PoC đo được | Kết quả |
|---|---|
| Dữ liệu | 100 000 request / 10 ngày / 50 tenant, gài 900 giá trị PII (email, SĐT, CCCD) |
| PII thô trong file Parquet trên đĩa (110 file, kể cả file trước Z-order đã tombstone) | **0** |
| Token tất định trong tenant / không link được giữa tenant | True / True |
| Query 1 tenant sau Z-order | **10/100 file** (1 file/ngày) |
| DELETE `event_date < cutoff` | 30 file bị gỡ, **0 dòng phải copy** (DELETE trùng ranh giới partition) |
| Time travel về version trước delete, **trước** VACUUM | vẫn trả 100 000 dòng → *deleted ≠ gone* |
| Sau VACUUM | 27.4 MB → 12.8 MB; time travel lỗi `FileNotFoundError`; Bronze còn 7 ngày, Gold còn 10 ngày |

PoC chưa chứng minh: chạy trên S3 thật, Spark Structured Streaming, phát hiện PII tự do (NER), và
throughput ở 35K req/s. Đó là AC1/AC5/AC6 của MVP.

## Nguồn tham khảo

- Delta Lake docs — VACUUM, Change Data Feed, deletion vectors & `REORG TABLE … APPLY (PURGE)`: <https://docs.delta.io/>
- Apache Iceberg spec — metadata/manifest list per snapshot: <https://iceberg.apache.org/spec/>
- Unity Catalog OSS — temporary credentials & grants: <https://www.unitycatalog.io/>
- AWS S3 pricing (Standard, Standard-IA minimum storage duration, requests): <https://aws.amazon.com/s3/pricing/>
- AWS EC2 on-demand & EBS gp3 pricing: <https://aws.amazon.com/ec2/pricing/on-demand/>, <https://aws.amazon.com/ebs/pricing/>
- Masson, Rim, Lee — *DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees*, VLDB 2019.
- Số đo trong lab này: NB1 (schema enforcement), NB2 (prune 55×), NB3 (RESTORE), NB4 (bẫy múi giờ),
  NB5 (metadata 290 % khi commit nhỏ), NB6 (VACUUM bỏ sót orphan, expiry không xóa file).
