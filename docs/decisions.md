# Nhật ký quyết định kiến trúc CĐTN

**Cập nhật 15/11/2026:** Giữ D16 và bổ sung D17–D19 cho hợp đồng Bronze, native streaming sink/checkpoint và đối soát có mốc. Code/DDL/Compose đã viết; chưa áp DDL hoặc khởi chạy ingest Kafka → Bronze. CDC snapshot từng cột và c/u/d/tombstone đã kiểm tra ngày 08/11; người dùng đã báo thực nghiệm phục hồi CDC và thao tác bảng Iceberg thử trong chat, còn lưu bằng chứng phù hợp trước khi đóng nghiệm thu. dbt-spark/SQL endpoint chưa triển khai.

## 1. Quyết định phạm vi

| ID | Quyết định | Lý do và hệ quả |
|---|---|---|
| D01 | Dự án nằm tại /home/thienduc/cdtn-lakehouse, ngang cấp với bigdata_project. | Musicify là dự án riêng; không nhập mã hoặc thay đổi Musicify khi làm CĐTN. |
| D02 | Dùng 8 CSV Olist: customers, sellers, products, orders, order_items, order_payments, order_reviews, product_category_name_translation. | Thiết kế schema và nạp đủ 8 bảng PostgreSQL trước CDC; đối soát từng tệp. CSV geolocation nằm ngoài phạm vi lõi. |
| D03 | Giữ 10 câu hỏi Q01, Q02, Q04, Q05, Q06, Q08, Q09, Q10, Q11, Q12. Ngừng Q03, Q07, Q13; giữ mã cũ. | Q03 là lát cắt phụ của Q01; Q07 phân mảnh/chồng lặp Q02 và Q05; Q13 khó diễn giải và không đủ cơ sở nhân quả. Gold/KPI/dashboard chỉ phải phục vụ 10 câu hỏi đang dùng. |
| D04 | Không nạp geolocation và không xây postal_locations trong lõi. | Phân tích bang dựa trên state hiện có trong customers/sellers. Thay đổi quyết định này chỉ khi câu hỏi địa lý cụ thể cần tọa độ/zip và đã có quy tắc làm sạch. |
| D05 | Luồng lõi cập nhật theo D16 ngày 13/11: PostgreSQL → Debezium/Kafka → Spark Structured Streaming → Iceberg Bronze → dbt qua Spark SQL tạo Iceberg Silver/Gold → Trino → Superset. | Spark xử lý dữ liệu; dbt quản lý model/phụ thuộc/tests; Trino phục vụ truy vấn. Thay phân vai Trino/dbt trước đây; phạm vi nguồn và 10 câu hỏi giữ theo D02/D03. |
| D06 | CDC 7 bảng nghiệp vụ; bảng dịch danh mục là tham chiếu tĩnh, Spark batch đọc PostgreSQL qua JDBC và ghi bảng Iceberg Silver theo D16. | Chạy lại batch có đối soát khi tham chiếu thay đổi; model dbt dùng bản Iceberg, không đòi Trino PostgreSQL connector cho đường nạp này. |
| D07 | Bronze giữ sự kiện CDC kể cả delete; Silver giữ trạng thái hiện tại; Gold giữ fact, dimension và KPI. | Không xóa bằng chứng lịch sử ở Bronze; xử lý trùng/update/delete trước khi tính chỉ số. |
| D08 | Bảng bền vững Bronze/Silver/Gold dùng Iceberg trên MinIO; Spark/Trino dùng cùng catalog. | Có snapshot và bảng chung cho engine. Model trung gian ưu tiên ephemeral; view chỉ dùng khi đã kiểm chứng hỗ trợ của catalog/engine đọc. |
| D09 | Superset gửi truy vấn tới Trino để đọc Iceberg Gold sau khi dbt qua Spark hoàn tất và tests đạt. | Không hứa dashboard thời gian thực; đo riêng độ trễ đến Bronze/Gold. Phân vai Trino đọc phải đi cùng phân quyền khi triển khai serving. |
| D10 | Airflow, Nessie, Prometheus/Grafana, Great Expectations là phần mở rộng. | Không để hạ tầng mở rộng cản luồng lõi. |
| D11 | Danh sách 10 repo ở architecture.md là nguồn tham khảo chọn lọc. | Áp dụng ý tưởng phù hợp sau khi đọc mã/tài liệu liên quan, kiểm tra phiên bản/giấy phép; không sao chép toàn bộ kiến trúc. |
| D12 | DDL PostgreSQL nguồn gồm đúng 8 bảng trong schema olist, giữ tên cột CSV; khóa order_reviews là (review_id, order_id). Products giữ product_category_name nullable và không có FK cứng sang bảng dịch. | Profiling CSV thật ngày 30/10/2026: review_id lặp 814 dòng, 610 products thiếu danh mục và 13 products thuộc 2 danh mục chưa dịch. Bảng nguồn đối soát 1:1 với CSV; chuẩn hóa tên cột và danh mục ở Silver/Gold. Xem docs/source-schema.md và sql/001_olist_source.sql. DDL đã chạy và 8 bảng đã nạp/đối soát ngày 01/11/2026. |
| D13 | CDC local dùng Kafka KRaft một broker, Kafka Connect/Debezium 3.7, publication `olist_cdc_pub` cho 7 bảng, slot `olist_cdc_slot`, snapshot `initial`, decimal chuỗi và prefix `olist_pg`; mật khẩu từ `.env`. | Ngày 08/11 đã đối soát 550.688 snapshot event theo số dòng/khóa/từng cột; thử `c/u/d` và tombstone rồi khôi phục nguồn. Người dùng đã thực nghiệm phục hồi, cần lưu bằng chứng offset/log để đóng nghiệm thu; xem `docs/runbook.md`. |
| D14 | Ngày 05/11 chọn MinIO Chainguard, PostgreSQL metadata riêng/JDBC V1 `lakehouse`, Spark 3.5.9/Scala 2.12 + Iceberg 1.12.0 + pgJDBC 42.7.14, Trino 484; Spark local 2 core, bucket `olist-lakehouse`. | Đọc lại cấu hình 13/11: MinIO server khóa digest; `minio-init` dùng `minio-client:latest-dev` có shell, chưa khóa digest; Spark đã đặt `spark.jars.ivy=/tmp/ivy-cache`. Đã khởi động và đọc catalog/namespace ở phiên trước; còn kiểm chứng Spark ghi → Trino đọc và giữ qua restart. |
| D15 | Ngày 08/11/2026 bật `key.converter.schemas.enable=true` và `value.converter.schemas.enable=true` cho connector đã tồn tại. | Snapshot cũ vẫn là JSON không kèm Connect schema; sự kiện mới có `schema`/`payload` trong cùng 7 topic. Không xóa volume, slot hoặc offset để ép chụp lại. Consumer Bronze và công cụ kiểm tra phải đọc được cả hai dạng; việc đổi converter không tự tạo Schema Registry. |
| D16 | Ngày 13/11/2026: người dùng duyệt Spark phụ trách ingest và biến đổi dữ liệu; dbt dùng `dbt-spark` gửi SQL cho Spark để tạo Silver/Gold; Trino là query engine cho Superset. Bảng dịch danh mục đi bằng Spark JDBC. | Thống nhất engine xử lý và phân vai với phục vụ BI. Streaming Bronze và batch dbt là các application/vòng đời riêng. Cần kiểm chứng SQL endpoint (ưu tiên Thrift), namespace/catalog JDBC V1, materialization, delete và chạy lại trước triển khai. Quyết định thay vai trò xử lý ở D05/D06; chưa xác nhận dbt-spark đã chạy hoặc khóa phiên bản. |


| D17 | Ngày 15/11/2026: 7 bảng `lakehouse.bronze.<table>` có chung schema CDC 24 cột theo `sql/004_olist_bronze.sql`; một bảng ứng với một topic. Giữ raw key/value BINARY, metadata Kafka/source, key/before/after JSON, op, schema flags, tombstone, parse_error và ingested_at UTC. | Đọc được JSON cũ và schema/payload mới; message lỗi không bị bỏ. Unpartitioned, format V2, Parquet/Zstd, target 134217728 bytes (128 MiB) là mốc ban đầu từng bảng, không ép file phải đạt kích thước đó. Không tạo UNIQUE/PK giả; identity Kafka được checker đối soát. |
| D18 | Ngày 15/11/2026: dùng native Iceberg streaming append sink, một query/checkpoint mỗi bảng. Service opt-in `spark-bronze` dùng image Spark hiện có, local[2]; mặc định orders/order_items, trigger 60 giây, maxOffsetsPerTrigger 100000 mỗi query. | Tái dùng checkpoint/query ID và epoch commit của Iceberg; volume `bronze_checkpoints` mount work-dir thuộc user spark. Marker topic/target/UUID, process lock, chốt thiếu checkpoint và failOnDataLoss ngăn replay sai phổ biến. Lần đầu yêu cầu Kafka earliest còn 0; không tự reset dữ liệu. Không khẳng định exactly-once toàn hệ thống trước kiểm thử recovery thật. |
| D19 | Ngày 15/11/2026: checker Bronze chỉ đọc, capture khoảng earliest/latest-exclusive từng partition và snapshot Iceberg từng bảng; so tập identity, raw bytes, metadata và cột parse. | Không suy số sự kiện từ hiệu offset, không so số dòng Bronze với PostgreSQL hiện tại. Mặc định từ chối audit lịch sử đầu nếu earliest > 0; cờ retained-only phải ghi rõ phạm vi hẹp. Snapshot r cần đối chiếu baseline và trạng thái nguồn/Silver riêng. Chưa chạy checker với dữ liệu thật trong lần triển khai. |

Các quyết định D02, D03, D04 và D06 cập nhật phạm vi cũ 9 CSV/13 câu hỏi theo yêu cầu ngày 29/10/2026 và câu trả lời mới nhất: “CDC bảng nghiệp vụ, nạp 8 bảng CSV trừ geolocation”.

## 2. P01–P04 đã chốt

| ID | Quyết định ngày 29/10/2026 | Lý do | Việc phải kiểm chứng khi triển khai |
|---|---|---|---|
| P01 | Profiling, thiết kế DDL và nạp đủ 8 bảng PostgreSQL trước; sau đó CDC 7 bảng nghiệp vụ. Dùng orders + order_items → Q01 làm luồng dọc đầu tiên sau nạp đủ nguồn. | Có nguồn đối soát đầy đủ và mốc tích hợp nhỏ để tìm lỗi sớm. | Snapshot từng cột và `c/u/d`/tombstone đã kiểm tra; phục hồi đã được người dùng thực nghiệm, còn lưu bằng chứng. Đối soát Q01 sau khi Bronze/Silver/Gold sẵn sàng. |
| P02 | Dùng Iceberg JDBC catalog với PostgreSQL metadata database riêng cho bản lõi; Nessie để sau. | Spark và Trino cùng phân giải bảng trên MinIO; phù hợp Compose cục bộ. D16 dùng Spark ghi và Trino đọc trong luồng chính. | Kiểm chứng Spark/dbt-spark tạo/ghi và Trino đọc, restart/metadata. Nếu không tương thích, ghi quyết định mới trước khi đổi REST; phép ghi Trino chỉ là kiểm thử khả năng engine tùy chọn. |
| P03 | Chạy dbt qua Spark bằng lệnh/script trong bản lõi; Airflow để sau. | Silver/Gold cập nhật theo từng lần build, streaming vào Bronze chạy riêng. | Chờ Bronze đến mốc → build/test Silver → build/test Gold → cho Superset đọc qua Trino; log/mã lỗi và chạy lại an toàn. |
| P04 | Tách Compose thành foundation, CDC, lakehouse, serving nhưng ghép bằng một Compose project cố định. | Dễ bật/kiểm tra theo nhóm mà các dịch vụ vẫn chung mạng và định danh; giảm tệp cấu hình quá lớn. | docker compose config, healthcheck, thứ tự khởi động, đường dẫn tương đối theo tệp base; down thường giữ volume, reset riêng. |

P01–P04 là **quyết định đã chốt**, không còn là “phương án chờ chọn”. Các phép kiểm chứng là điều kiện để khẳng định triển khai hoạt động, không phải điều kiện để hiểu quyết định thiết kế.

## 3. Câu hỏi kỹ thuật và phép kiểm chứng còn mở

O01 (schema nguồn và khóa) đã chốt ở D12. Snapshot từng cột và `c/u/d`/tombstone đã kiểm tra ngày 08/11; phục hồi CDC đã được người dùng báo hoàn thành nhưng còn thiếu bằng chứng lưu trong repo. D16 đã chốt engine xử lý Silver/Gold là Spark; tương thích dbt-spark và cách triển khai endpoint còn cần kiểm chứng.

| ID | Câu hỏi cần giải quyết | Khi nào chốt |
|---|---|---|
| O02 | Converter/topic/tombstone/snapshot/publication/quyền ở D13/D15 đã kiểm tra; người dùng đã thực nghiệm phục hồi. | Đóng hồ sơ nghiệm thu sau khi lưu offset trước/sau và bằng chứng sự kiện mới tiếp tục đến. |
| O03 | Hợp đồng Bronze đã chốt ở D17–D19. Thứ tự sự kiện cùng khóa qua partition/topic khác nhau và mốc đầu vào Silver vẫn cần quy định theo nguồn. | Nghiệm thu native ingest/checkpoint trước Silver; không coi offset là thứ tự toàn cục. |
| O04 | Silver qua dbt-spark xử lý lặp/update/delete và mốc nhất quán ra sao; full build hay incremental/merge theo model? | Trước triển khai Silver/Gold; full build chọn sự kiện cuối rồi lọc delete, incremental phải có cơ chế xóa tường minh. |
| O05 | Grain, công thức, bộ lọc và mốc thời gian của 10 câu hỏi đang dùng là gì? Q01 cần chốt giá trị hàng/freight/đơn đã giao; Q09 cần giới hạn cách diễn giải mua lặp lại. | Trước Gold/dashboard tương ứng. |
| O06 | Mốc dữ liệu nhất quán cho đối soát PostgreSQL ↔ Silver và nguồn ↔ Gold xác định thế nào? | Trước chương trình đối soát. |
| O07 | Kafka/Debezium 3.7, Spark 3.5.9, Iceberg 1.12.0 và Trino 484 đã chọn. Phiên bản dbt Core/dbt-spark, SQL endpoint và namespace cho catalog `lakehouse` còn mở theo D16. | Kiểm chứng dbt tạo/đọc/chạy lại model Iceberg trên JDBC V1 và Trino đọc cùng kết quả; sau đó khóa dependency/cấu hình cho triển khai. |
| O08 | Tiếp tục giữ 8 CSV gốc ở `data/raw/`; chặng Bronze hiện đọc Kafka, chưa tạo thêm bản sao CSV trong MinIO/raw. | Đánh giá riêng khi thiết kế backup/lưu trữ nguồn; không cản luồng Kafka → Bronze. |
| O09 | Ngưỡng nghiệm thu cho độ trễ, thông lượng, idempotency và phục hồi là gì? | Trước thực nghiệm chính thức. |
| O10 | Streaming đã có service riêng theo D18; SQL endpoint và dbt runner vẫn chưa triển khai. | Kiểm chứng startup/recovery/tài nguyên Bronze, rồi POC Thrift/dbt; không mặc định thêm master/worker cho local. |

## 4. Quy tắc cập nhật

- Ghi ngày, lý do, phạm vi ảnh hưởng và phép kiểm chứng khi thay đổi quyết định. Nếu đổi luồng/tầng dữ liệu, cập nhật docs/architecture.md; nếu đổi phạm vi hoặc cách làm việc dài hạn, cập nhật AGENTS.md.
- Docs/business-questions.md và docs/kpi-dictionary.md, khi được tạo, là nơi ghi định nghĩa chi tiết. Không biến bảng quyết định thành tài liệu công thức.
- Không ghi “đã kiểm chứng” nếu chưa có lệnh chạy, kết quả và dữ liệu đối chiếu.
- Giữ danh sách repo và đánh giá mức độ áp dụng tại docs/architecture.md mục 8; đường dẫn generator chưa xác minh nội dung nên chưa dùng làm căn cứ thiết kế.

## Tài liệu kỹ thuật gốc

- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Iceberg catalog trong Spark](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Trino JDBC catalog và lưu ý tương thích](https://trino.io/docs/current/object-storage/metastores.html)
- [Spark JDBC data source](https://spark.apache.org/docs/3.5.9/sql-data-sources-jdbc.html)
- [Spark Thrift Server](https://spark.apache.org/docs/3.5.9/sql-distributed-sql-engine.html)
- [dbt-spark: Iceberg và incremental](https://docs.getdbt.com/reference/resource-configs/spark-configs)
- [Docker Compose nhiều tệp](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/)