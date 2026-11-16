# Kiến trúc hệ thống CĐTN Lakehouse

**Cập nhật triển khai 15/11/2026:** Giữ luồng Spark streaming → Bronze; dbt qua Spark SQL → Silver/Gold; Trino phục vụ truy vấn. PostgreSQL đã nạp/đối soát 8 CSV; CDC đã đối soát 550.688 snapshot event từng cột và thử `c/u/d`/tombstone ngày 08/11. Người dùng đã báo thực nghiệm phục hồi CDC và thao tác bảng Iceberg thử qua Spark/Trino trong phiên chat; chưa lưu đầy đủ bằng chứng restart. Đã viết DDL 7 bảng Bronze, job ingest, checker và service `spark-bronze` theo D17–D19; chưa áp DDL hoặc chạy ingest Kafka → Bronze trong lần triển khai này. Không suy ra trạng thái dịch vụ hiện tại từ các mốc trên.

## 1. Mục tiêu, dữ liệu và câu hỏi

Xây dựng lakehouse mã nguồn mở để phân tích thương mại điện tử Olist. Phạm vi lõi dùng 8 trong 9 CSV: customers, sellers, products, orders, order_items, order_payments, order_reviews và product_category_name_translation. Tệp geolocation không nạp vào PostgreSQL và không có bảng postal_locations trong bản lõi. Các phân tích theo bang dùng customer_state hoặc seller_state sẵn có; không suy diễn tọa độ từ zip code.

Giữ 10 câu hỏi theo mã cũ để truy vết thảo luận:

| Mã | Câu hỏi kinh doanh đang dùng |
|---|---|
| Q01 | Số đơn và giá trị theo tháng thay đổi thế nào? |
| Q02 | Các bang đóng góp bao nhiêu đơn và giá trị? |
| Q04 | Tỷ lệ đơn delivered, cancelled và unavailable là bao nhiêu? |
| Q05 | Danh mục sản phẩm nào dẫn đầu về số đơn và giá trị? |
| Q06 | Tỷ trọng danh mục thay đổi theo thời gian thế nào? |
| Q08 | Doanh số tập trung ở các seller ra sao? |
| Q09 | Giá trị khách hàng và tỷ lệ mua lặp lại ra sao? |
| Q10 | Tỷ lệ giao đúng hạn là bao nhiêu? |
| Q11 | Thời gian ở các chặng giao hàng là bao lâu? |
| Q12 | Đơn giao trễ có điểm review khác biệt thế nào? |

Loại Q03 vì so sánh số đơn với giá trị đơn trung bình có thể là phân tích phụ của Q01; loại Q07 vì ma trận danh mục × bang dễ phân mảnh và chồng lặp Q02/Q05; loại Q13 vì trọng lượng/phí vận chuyển so với giao trễ dễ bị diễn giải nhầm thành quan hệ nhân quả. Q03/Q07/Q13 không là KPI hoặc dashboard bắt buộc. Q09 là mô tả lịch sử mua lặp lại trong Olist, không suy rộng thành mô hình retention/churn khi dữ liệu không đủ. Định nghĩa grain, bộ lọc và công thức từng câu hỏi sẽ được ghi trong docs/business-questions.md và docs/kpi-dictionary.md khi tạo.

## 2. Luồng dữ liệu logic

~~~mermaid
flowchart LR
    CSV["8 CSV Olist (trừ geolocation)"] --> LOAD["Schema + loader + đối soát"]
    LOAD --> PG[("PostgreSQL nguồn: 8 bảng")]
    GEN["Generator: thêm / sửa / xóa"] --> PG
    PG -->|7 bảng nghiệp vụ| DBZ["Debezium PostgreSQL"]
    DBZ --> K["Kafka topics"]
    K --> STREAM["Spark Structured Streaming"]
    STREAM --> B[("Iceberg Bronze: CDC events")]
    B --> SS["Spark SQL: chuẩn hóa + áp dụng CDC"]
    PG -->|bảng dịch danh mục| JDBC["Spark batch JDBC"]
    JDBC --> S[("Iceberg Silver: trạng thái hiện tại + tham chiếu")]
    SS --> S
    S --> GS["Spark SQL: mô hình phân tích"]
    GS --> G[("Iceberg Gold: fact, dimension, KPI")]
    DBT["dbt-spark: model, phụ thuộc, tests"] -. SQL .-> SS
    DBT -. SQL .-> GS
    G --> TR["Trino: phục vụ truy vấn"]
    TR --> BI["Superset"]
~~~

Bảy bảng CDC là customers, sellers, products, orders, order_items, order_payments và order_reviews. Bảng product_category_name_translation là tham chiếu tĩnh: nạp vào PostgreSQL, sau đó Spark batch đọc `olist.product_category_name_translation` bằng JDBC và ghi thành bảng Iceberg tham chiếu ở Silver. Model dbt chạy trên Spark dùng bảng Iceberg này khi chuẩn hóa/làm giàu dữ liệu. Nếu nguồn tham chiếu thay đổi, chạy lại batch có đối soát; không giả định bảng tự cập nhật qua CDC.

MinIO lưu data/metadata files của Iceberg Bronze, Silver và Gold. Iceberg JDBC catalog tên `lakehouse` dùng chung cho Spark và Trino lưu định danh bảng và con trỏ metadata; database catalog tách khỏi PostgreSQL nghiệp vụ. Spark ghi các tầng dữ liệu; Trino đọc trực tiếp các bảng Iceberg Gold để trả kết quả SQL cho Superset, không cần sao chép Gold sang kho riêng. Đây là phân vai xử lý/phục vụ; cấu hình phân quyền Trino chỉ đọc cần được triển khai và kiểm chứng riêng.

## 3. Trách nhiệm từng tầng và định dạng

| Thành phần | Đầu vào → đầu ra | Trách nhiệm |
|---|---|---|
| Loader | 8 CSV → 8 bảng PostgreSQL | Xác thực cột/kiểu, thiết kế khóa sau profiling, nạp có thể chạy lại, ghi số dòng hợp lệ/lỗi và đối soát quan hệ. |
| PostgreSQL nguồn | Dữ liệu lịch sử + thay đổi mô phỏng | Giữ bảng nghiệp vụ có khóa ổn định; bật logical replication cho đúng 7 bảng CDC. |
| Debezium + Kafka | PostgreSQL WAL → topic theo bảng | Snapshot ban đầu cho 7 bảng, sau đó phát insert/update/delete đã commit; bảng dịch danh mục không nằm trong table include list. |
| Spark Structured Streaming | Kafka → Iceberg Bronze | Parse envelope, ghi append-only và lưu topic/partition/offset, bảng, khóa, thao tác, metadata nguồn và thời điểm ingest. |
| Iceberg Bronze | Sự kiện CDC | Lưu dấu vết thay đổi, kể cả delete; không dùng trực tiếp để tính KPI trạng thái hiện tại. |
| dbt + Spark SQL Silver | Bronze + bảng dịch danh mục Iceberg → Iceberg Silver | dbt quản lý model/phụ thuộc/tests và gửi SQL; Spark chuẩn hóa kiểu, loại trùng, áp dụng update/delete theo khóa để tạo trạng thái hiện tại. |
| dbt + Spark SQL Gold | Silver → Iceberg Gold | Spark thực thi SQL do dbt biên dịch để xây fact/dimension và KPI cho 10 câu hỏi; chỉ phát hành kết quả sau tests. |
| Trino | Iceberg Gold → kết quả SQL | Query engine cho Superset; dùng chung JDBC catalog/MinIO với Spark và không tham gia biến đổi Silver/Gold trong luồng đã chốt. |
| Superset | Kết quả truy vấn Trino trên Gold → dashboard | Hiển thị dữ liệu sau mỗi lần xử lý Silver/Gold thành công; không cam kết thời gian thực. |

PostgreSQL nguồn giữ schema olist với 8 bảng và tên cột theo CSV để nạp/đối soát trực tiếp. Bảng dịch danh mục không được dùng làm FK cứng của products vì CSV thật có 13 sản phẩm thuộc hai danh mục chưa dịch; xử lý bằng LEFT JOIN ở Silver/Gold. order_reviews dùng khóa ghép (review_id, order_id) vì review_id lặp. Chi tiết và DDL ở docs/source-schema.md và sql/001_olist_source.sql.

Iceberg áp dụng cho **các bảng được lưu bền vững ở cả ba tầng**. dbt dùng adapter `dbt-spark`; model trung gian có thể dùng `ephemeral`, còn view chỉ dùng sau khi kiểm chứng catalog và engine đọc hỗ trợ. Silver/Gold là bảng Iceberg materialized. Với Olist cỡ nhỏ, ưu tiên full build để có mốc kết quả dễ đối chiếu, sau đó mới chọn incremental/merge theo model. Silver chọn sự kiện cuối theo khóa trước khi lọc delete; `merge` mặc định của dbt không tự áp dụng `op=d`, nên chiến lược xóa và chạy lại phải được thiết kế tường minh. Bronze vẫn giữ sự kiện xóa và tombstone.

## 4. Chất lượng, nhất quán và phục hồi

- Trước khi áp ràng buộc PostgreSQL, profiling từng CSV để xác nhận khóa dự kiến, đặc biệt khóa ghép của order_items/order_payments và tính duy nhất của review_id. Nếu dữ liệu thật vi phạm, ghi quy tắc xử lý hoặc khóa thay thế ổn định; không âm thầm bỏ dòng.
- Loader đối chiếu số dòng nguồn, số dòng nạp, dòng lỗi, khóa trùng và quan hệ giữa 8 bảng. Giữ báo cáo nạp theo từng lần chạy.
- Bronze nhận diện bản ghi Kafka bằng topic + partition + offset; kiểm tra metadata Debezium phục vụ sắp thứ tự thay đổi trên từng khóa. Silver phải chống sự kiện lặp và xử lý delete; không giả định exactly-once xuyên toàn bộ pipeline.
- dbt tests kiểm tra unique/not_null/relationships/accepted values và quy tắc nghiệp vụ ở Silver/Gold. Đối soát PostgreSQL với Silver sau khi CDC bắt kịp một mốc đã chọn; đối chiếu Gold bằng truy vấn độc lập ở cùng mốc.
- Thử restart job Spark ingest/Kafka, tiếp tục từ checkpoint và đối soát Bronze; thử chạy lại dbt qua Spark để xác nhận Silver/Gold không nhân đôi. Đo riêng độ trễ PostgreSQL → Bronze và PostgreSQL → Gold.
- Ghi kịch bản, dữ liệu và ngưỡng nghiệm thu vào docs/experiments.md trước khi tuyên bố pipeline đạt yêu cầu.

## 5. Hạ tầng và điều phối

Tách Compose thành foundation (PostgreSQL nguồn, MinIO, JDBC catalog database), CDC (Kafka, Kafka Connect/Debezium), lakehouse (runtime Spark xử lý dữ liệu và Trino phục vụ truy vấn) và serving (Superset dự kiến). dbt runner thuộc khâu biến đổi chạy qua Spark; cách đóng gói runner và SQL endpoint chưa được triển khai. Ghép các tệp dưới **một Compose project cố định** để dùng chung mạng, service name và dependency. Kiểm tra cấu hình bằng `docker compose config --quiet`, dùng healthcheck và giữ reset dữ liệu thành thao tác riêng.

Cụm CDC dùng `infra/compose.cdc.yml` cùng Compose project với `infra/compose.foundation.yml`; publication ở `sql/002_olist_cdc.sql`; mẫu connector ở `infra/debezium/olist-postgres.template.json`; đăng ký và đối soát ở `scripts/olist_cdc.py`, `scripts/check_olist_cdc.py`. Runbook ghi tách biệt quy trình sau restart và thiết lập lần đầu. Snapshot ban đầu là JSON không kèm Connect schema; connector hiện phát sự kiện mới dạng `schema`/`payload`. Hai dạng cùng tồn tại trong topic và Bronze phải đọc được cả hai.

Tầng lakehouse local dùng `postgres-catalog` và MinIO trong foundation, Spark local runner và Trino trong `infra/compose.lakehouse.yml`. Hai engine dùng catalog JDBC V1 tên `lakehouse` và warehouse `s3://olist-lakehouse/warehouse`. `sql/003_iceberg_catalog.sql` tạo metadata tables khi volume catalog mới khởi tạo. Các dịch vụ đã được khởi động ở phiên trước; người dùng đã chạy thành công `SHOW NAMESPACES IN lakehouse` qua Spark sau khi đặt Ivy cache ở `/tmp/ivy-cache`. Người dùng đã quan sát ghi/thay đổi bảng Iceberg thử và Trino đọc trong phiên chat; đây chưa là nghiệm thu Kafka → Bronze hoặc dbt-spark. Healthcheck runner chỉ xác nhận file cấu hình.

Trong bản lõi, chạy dbt qua Spark bằng lệnh/script: chờ Bronze đến mốc dữ liệu đã chọn → build/test Silver → build/test Gold → cho dashboard đọc qua Trino. Mỗi lần chạy là một đợt batch; job streaming vào Bronze có vòng đời riêng. Ghi mã thoát và log; không công nhận hoàn tất nếu một bước hoặc test lỗi. Airflow, Nessie, Prometheus/Grafana và Great Expectations chỉ thêm sau khi luồng lõi có bằng chứng kiểm chứng.

### 5.1. Bản đồ file và vòng đời cấu hình

Bảng này truy từ file trong repo đến tiến trình sử dụng và nơi giữ trạng thái. Mức kiểm chứng phản ánh bằng chứng đã ghi trong [runbook](runbook.md); cấu hình đã viết chưa đồng nghĩa dịch vụ hoặc luồng dữ liệu đã chạy.

| Khâu / dịch vụ | File nguồn → nơi sử dụng | Khi nào có tác dụng | Trạng thái dữ liệu và kiểm chứng |
|---|---|---|---|
| Profiling và nạp nguồn | `data/raw/*.csv` → [`scripts/profile_olist.py`](../scripts/profile_olist.py) → `data/olist_profile.json`; [`sql/001_olist_source.sql`](../sql/001_olist_source.sql) tạo bảng; [`scripts/load_olist.py`](../scripts/load_olist.py) đọc CSV và profile. | Chạy profiling/DDL/load theo mục thiết lập lần đầu trong runbook; `load` chỉ ghi khi gọi tường minh và yêu cầu 8 bảng rỗng. | 8 bảng trong `postgres_source_data`; đã nạp và đối soát. `data/` và `.env` bị Git bỏ qua. |
| PostgreSQL nguồn | [`compose.foundation.yml`](../infra/compose.foundation.yml) khai báo `postgres-source`, mount `postgres_source_data:/var/lib/postgresql/data` và đặt `wal_level=logical`. | Compose khởi động container; PostgreSQL tiếp tục từ volume qua restart. | Dữ liệu 8 bảng và WAL ở volume nguồn; CDC đọc 7 bảng nghiệp vụ. |
| Publication CDC | [`sql/002_olist_cdc.sql`](../sql/002_olist_cdc.sql) được chạy bằng `psql` theo runbook, sau khi nạp nguồn. | Tạo publication `olist_cdc_pub` cho đúng 7 bảng; không tự chạy khi khởi động Compose. | Publication nằm trong PostgreSQL nguồn; đã kiểm tra. |
| Kafka | [`compose.cdc.yml`](../infra/compose.cdc.yml) khai báo broker `kafka`, mount `kafka_data:/kafka/data`. | Broker chạy cùng Compose project và giữ log topic qua restart nếu giữ volume. | 7 topic `olist_pg.olist.<table>` chứa snapshot và thay đổi; Connect còn dùng các topic nội bộ cho config, offset, status. Đã đối soát snapshot và `c/u/d`. |
| Debezium Connect | [`compose.cdc.yml`](../infra/compose.cdc.yml) chạy `connect`; [`olist-postgres.template.json`](../infra/debezium/olist-postgres.template.json) được [`scripts/olist_cdc.py`](../scripts/olist_cdc.py) điền biến rồi gửi qua REST API. | Đăng ký connector một lần; Connect đọc publication qua slot `olist_cdc_slot` và phát vào Kafka. Sửa template không tự cập nhật connector đã đăng ký. | Connector/slot đã kiểm tra; [`scripts/check_olist_cdc.py`](../scripts/check_olist_cdc.py) dùng để đối soát. |
| PostgreSQL catalog | [`compose.foundation.yml`](../infra/compose.foundation.yml) mount [`sql/003_iceberg_catalog.sql`](../sql/003_iceberg_catalog.sql) vào `/docker-entrypoint-initdb.d/001_iceberg_catalog.sql:ro` của `postgres-catalog`. | Chỉ khởi tạo metadata JDBC V1 khi `postgres_catalog_data` mới; sửa SQL không tự áp vào volume có sẵn. | Đã thấy hai bảng metadata ở phiên kiểm tra trước; còn kiểm chứng con trỏ bảng thật khi Spark ghi và Trino đọc. |
| MinIO | [`compose.foundation.yml`](../infra/compose.foundation.yml) mount `minio_data:/data`; `minio-init` tạo bucket `olist-lakehouse`. | MinIO giữ object; `minio-init` thoát mã 0 trước Spark/Trino. Image init hiện là `minio-client:latest-dev` để có shell, chưa khóa digest. | Bucket đã được tạo ở phiên trước; warehouse là `s3://olist-lakehouse/warehouse`. Chưa có bằng chứng bảng Bronze/Silver/Gold đã được ghi. |
| Spark runner | [`compose.lakehouse.yml`](../infra/compose.lakehouse.yml) mount [`spark-defaults.conf`](../infra/spark/spark-defaults.conf), SQL Bronze và hai script Bronze chỉ đọc; copy config sang `/tmp/spark-conf` rồi thêm mật khẩu từ env. | `spark-sql` áp DDL khi người dùng gọi; `spark-submit` chạy checker chỉ đọc. Runner vẫn chờ bằng `sleep infinity`. | Đã có code/mount; không tự chạy ingestion. SQL endpoint cho dbt còn chờ. |
| DDL Bronze | [`sql/004_olist_bronze.sql`](../sql/004_olist_bronze.sql) → `/opt/olist/sql/004_olist_bronze.sql` trong runner. | Chạy tường minh qua Spark SQL, tạo namespace `bronze` và 7 bảng cùng schema sự kiện; `IF NOT EXISTS` không migrate schema. | Format V2, unpartitioned, Parquet/Zstd, target 128 MiB; chưa áp DDL trong lần viết code. |
| Spark Bronze | [`scripts/ingest_olist_bronze.py`](../scripts/ingest_olist_bronze.py) → `/opt/olist/scripts/ingest_olist_bronze.py`; service `spark-bronze` dùng profile `bronze`. | Chỉ bắt đầu khi gọi tường minh `up spark-bronze`; một native Iceberg streaming query/checkpoint cho mỗi bảng được chọn. Mặc định `orders,order_items`, có thể mở đủ 7 bảng. | Volume `bronze_checkpoints` mount `/opt/spark/work-dir`, checkpoint con theo bảng; có identity/UUID guard và khóa tiến trình. Chưa chạy ingest/khôi phục thật. |
| Đối soát Bronze | [`scripts/check_olist_bronze.py`](../scripts/check_olist_bronze.py) chạy bằng Spark runner. | `bounds/status/verify` chỉ đọc Kafka và Iceberg; `validate` không khởi động Spark. | So tập sự kiện theo topic/partition/offset, byte gốc và mọi cột parse tại khoảng offset/snapshot cố định; không thay thế đối soát PostgreSQL ↔ Silver. |
| Trino | [`compose.lakehouse.yml`](../infra/compose.lakehouse.yml) mount [`iceberg.properties`](../infra/trino/catalog/iceberg.properties) vào `/etc/trino/catalog/lakehouse.properties:ro`. | Trino nạp Iceberg connector, lấy credentials từ env và dùng cùng catalog/warehouse với Spark. | Đã thấy catalog ở phiên trước; vai trò đích là đọc Gold cho Superset. Chưa có bằng chứng đọc bảng do dbt-spark tạo. |
| dbt qua Spark (dự kiến) | Chưa có dbt project, profile kết nối hay SQL endpoint được cấu hình trong Compose. | dbt biên dịch model SQL và gửi tới Spark; hướng kết nối ưu tiên kiểm chứng là Spark Thrift Server. | Phiên bản dbt Core/adapter, namespace, materialization và tài nguyên chạy cần chốt sau POC; chưa có lệnh vận hành chính thức. |

`--env-file .env` cấp giá trị để Compose thay biến; chỉ các biến được khai báo trong `environment` mới được truyền vào từng container. Các script Python trên host đọc `.env` trực tiếp; [`requirements-loader.txt`](../requirements-loader.txt) chỉ cài thư viện Python cho các script, còn JAR Iceberg/Kafka/JDBC của Spark được khai báo trong `spark-defaults.conf`. Các đường mount file cấu hình là **bind mount chỉ đọc**; `postgres_source_data`, `kafka_data`, `postgres_catalog_data`, `minio_data` và `bronze_checkpoints` là **named volume giữ trạng thái**. Riêng checkpoint Bronze là filesystem local trên volume, không dựa vào Iceberg S3FileIO.

Các lệnh thiết lập lần đầu, khởi động hằng ngày và kiểm tra lakehouse hiện có ở [runbook](runbook.md). Thử nghiệm lựa chọn/kết nối `dbt-spark` được hướng dẫn riêng trong phiên chat; không thêm thủ tục POC vào runbook ở lần cập nhật này. Code Kafka → Bronze đã được viết; áp DDL, khởi chạy và đối soát theo mục 5 của runbook trước khi công nhận đạt. Các model Silver/Gold qua Spark/dbt vẫn là chặng sau.

### 5.2. Vòng đời Spark streaming và Spark SQL/dbt

Job Spark Structured Streaming đọc Kafka liên tục và ghi Bronze theo micro-batch có checkpoint. dbt chạy từng đợt SQL để dựng Silver/Gold; không tự trở thành consumer Kafka và không dùng checkpoint streaming làm trạng thái model dbt. Hai khâu là các Spark application riêng, có log, tài nguyên và cách chạy lại riêng; dùng cùng image không tự tạo sự cách ly CPU/RAM trên máy WSL.

Hướng cần kiểm chứng cho dbt là Spark Thrift Server dùng cùng cấu hình JDBC catalog/S3 như runner hiện tại. dbt-spark dùng namespace qua `schema`; phiên Spark SQL phải phân giải tên vào catalog `lakehouse` (ví dụ default catalog tương ứng). Không suy ra adapter đã tương thích JDBC V1 chỉ từ việc `spark-sql` chạy được. SQL endpoint, phiên bản adapter và cách đóng gói runner còn chờ kiểm chứng; không bắt buộc thêm master/worker cho bản local chỉ vì dùng dbt.

Chốt rõ phạm vi mỗi lần build và mốc đầu vào trước khi đối soát. Với full build Silver, giữ thứ tự chọn sự kiện cuối theo khóa rồi mới loại bản ghi bị xóa; với incremental, phải xử lý delete, sự kiện lặp và chạy lại tường minh. Không dùng giá trị offset của các partition/topic khác nhau như một thứ tự toàn cục.


### 5.3. Hợp đồng Bronze và ranh giới khôi phục

Mỗi topic `olist_pg.olist.<table>` ánh xạ tới `lakehouse.bronze.<table>`. Bảy bảng là customers, sellers, products, orders, order_items, order_payments, order_reviews; bảng dịch danh mục tiếp tục đi batch JDBC sang Silver. DDL tạo đủ 7 bảng; mặc định job chỉ chạy orders/order_items để kiểm chứng luồng dọc trước. Không tự tạo bảng bằng job streaming.

Bronze giữ raw key/value kiểu BINARY đúng byte Kafka, metadata topic/partition/offset/timestamp, key JSON đã bỏ Connect wrapper, op, metadata source, before/after JSON và thời điểm ingest UTC. Message lỗi vẫn được ghi cùng `parse_error`; tombstone là Kafka value NULL, khác chuỗi JSON `null`. Parser hỗ trợ JSON snapshot cũ và `schema/payload` mới; kiểu nghiệp vụ chi tiết được chuẩn hóa ở Silver. Topic/partition/offset là identity sự kiện, không là khóa nghiệp vụ hay thứ tự toàn cục.

Job dùng native Iceberg append sink, tránh tự triển khai `foreachBatch` nhiều bảng. Iceberg theo dõi streaming query ID/epoch ID trong snapshot để nhận diện commit đã hoàn tất khi retry cùng checkpoint. Từng bảng có checkpoint riêng, marker gồm topic/bootstrap/target/table UUID và khóa filesystem chống hai process dùng cùng checkpoint volume. Bảng đã có dữ liệu nhưng thiếu checkpoint/offset log bị từ chối; đổi UUID/topic với checkpoint cũ cũng bị từ chối. Các chốt này không thay thế backup hoặc bảo vệ trước mọi trường hợp checkpoint hỏng.

Lần đầu dùng `startingOffsets=earliest`, yêu cầu earliest còn 0; khi resume, Spark dùng checkpoint. `failOnDataLoss=true` không cho âm thầm bỏ qua offset mất do retention. Native sink không bảo đảm dữ liệu từ PostgreSQL đầy đủ khi Kafka đã mất lịch sử; không tự reset topic, slot hoặc chụp lại snapshot. Không expire/rollback snapshot streaming tùy tiện vì cơ chế nhận diện epoch phụ thuộc lịch sử commit còn phù hợp.

Checker chốt earliest/latest-exclusive của Kafka và snapshot từng bảng, so tập identity thật thay vì lấy hiệu offset làm số message; Kafka có thể có khoảng trống offset. Đếm thiếu/thừa/trùng, so raw bytes và các cột parse; `ingested_at` được kiểm tra không NULL, không so với thời gian Kafka. Sự kiện đến sau mốc capture nằm ngoài phạm vi và được báo riêng. Bảng đang trễ có thể báo thiếu: kiểm tra progress rồi chạy lại; không đổi checkpoint để chữa bằng cách nạp chồng. Chưa có bằng chứng nghiệm thu nguồn → Bronze trong lần viết code.

Nguồn cơ chế: [Iceberg streaming writes](https://iceberg.apache.org/docs/latest/spark-structured-streaming/) và [SparkWrite của Iceberg 1.12.0](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java).

## 6. Thứ tự triển khai và mốc kiểm chứng

Bước profiling 8 CSV đã thực hiện ngày 30/10/2026; PostgreSQL đã nạp/đối soát ngày 01/11. Ngày 08/11 snapshot 7 bảng đã khớp số lượng, PK và từng cột, thử `c/u/d`/tombstone và khôi phục nguồn. Người dùng đã báo hoàn thành thử phục hồi CDC sau restart, cần lưu lại offset/log trước khi đóng hồ sơ nghiệm thu. Ngày 13/11 chốt Spark/dbt xử lý Silver/Gold và Trino phục vụ truy vấn; đây là thay đổi thiết kế, chưa phải kết quả chạy dbt.

1. Profiling cả 8 CSV: cột, kiểu, khóa ứng viên, quan hệ, null, trùng và phạm vi thời gian. Thiết kế DDL PostgreSQL cho **đủ 8 bảng**, thứ tự nạp theo phụ thuộc và chính sách xử lý lỗi; không tạo bảng geolocation.
2. Viết loader, nạp đủ 8 bảng vào PostgreSQL và đối soát từng tệp. Kiểm tra chạy lại không nhân đôi. Tạo bản sao/mốc nguồn để phân biệt dữ liệu gốc với thay đổi mô phỏng.
3. Bật logical replication và đăng ký Debezium cho 7 bảng nghiệp vụ. Snapshot từng cột và `c/u/d`/tombstone đã kiểm tra; bổ sung bằng chứng cho phép phục hồi CDC người dùng đã thực nghiệm. Bảng dịch danh mục đi theo batch Spark JDBC.
4. Áp DDL Bronze và chạy service `spark-bronze` cho orders/order_items; đối soát Kafka → Bronze, Trino đọc, restart giữ checkpoint rồi mở đủ 7 bảng. Code đã có, kết quả chạy thật còn chờ. Kiểm chứng riêng dbt-spark với JDBC catalog V1 trước Silver/Gold.
5. Xây Silver bằng dbt qua Spark cho các bảng phục vụ 10 câu hỏi; bảng dịch danh mục được Spark JDBC nạp thành Iceberg tham chiếu. Kiểm tra trạng thái sau update/delete, khóa và đối soát PostgreSQL ở cùng mốc.
6. Xây Gold bằng dbt qua Spark và dashboard Superset truy vấn qua Trino: Q01 làm luồng dọc đầu tiên, rồi mở rộng đến 9 câu hỏi còn lại. Chốt KPI dictionary và chạy tests/đối soát trước khi công bố từng nhóm.
7. Chạy thực nghiệm phục hồi, độ trễ và chạy lại; chỉ thêm Airflow hoặc thành phần mở rộng nếu có nhu cầu đã đo được.

## 7. Lựa chọn kỹ thuật và phần cần kiểm chứng

| Vấn đề | Hướng đã chọn | Phép kiểm chứng còn cần |
|---|---|---|
| Phạm vi nguồn/CDC | Nạp 8 bảng PostgreSQL; CDC 7 bảng nghiệp vụ; bảng dịch danh mục batch; bỏ geolocation. | Snapshot 550.688 sự kiện theo PK/từng cột và `c/u/d`/tombstone đã đạt. Người dùng đã thử phục hồi; còn lưu bằng chứng offset/log. |
| Iceberg | Bronze/Silver/Gold trên MinIO; Spark ghi, Trino đọc qua cùng JDBC catalog V1. | Bảng thật Spark → Trino, metadata và restart; nếu tương thích không đạt, ghi quyết định mới trước khi đổi catalog. |
| Bronze | Append-only envelope CDC và metadata truy vết; topic/partition/offset là định danh bản ghi Kafka. | Parser phải hỗ trợ snapshot JSON không schema và sự kiện mới có `schema`/`payload`; giữ tombstone/delete, kiểm tra ordering trên từng khóa và checkpoint trước khi xây Silver. |
| Silver/Gold | dbt qua Spark SQL tạo Iceberg; full build làm mốc đối chiếu trước, incremental/merge chọn theo model. | dbt-spark với JDBC V1, metadata/namespace, chạy lại, delete và kết quả Trino đọc; chốt phiên bản sau kiểm chứng. |
| Điều phối | Script/lệnh dbt trước; Airflow là mở rộng. | Mốc dữ liệu, chạy lỗi, chạy lại và phát hành dashboard sau tests. |
| Compose | Các file theo nhóm, một project, healthcheck và reset riêng. | docker compose config, thứ tự khởi động và bảo toàn volume. |

Schema nguồn và khóa đã chốt sau profiling và nạp thật. Cụm CDC local Debezium/Kafka 3.7 đã chạy với topic prefix `olist_pg`, decimal dạng chuỗi; từ 08/11 connector dùng Connect schema cho sự kiện mới trong khi snapshot lịch sử không có schema. Đối soát 550.688 snapshot event với PostgreSQL hiện tại đạt 0 khóa thiếu/thừa/trùng và 0 dòng/cột lệch. Phép thử phục hồi, quy tắc ordering trong Bronze/Silver, grain/công thức KPI và ngưỡng nghiệm thu vẫn được theo dõi trong docs/decisions.md.

## 8. Repo GitHub tham khảo chọn lọc

Danh sách sau là nguồn để học **một kỹ thuật phù hợp mỗi lần**, không phải mẫu kiến trúc để sao chép. Các gợi ý dưới đây dựa trên mô tả/README hoặc tệp được chỉ rõ; phải đọc phần mã tương ứng trước khi áp dụng.

| Repo | Điều nên xem xét áp dụng | Giới hạn khi áp dụng |
|---|---|---|
| [airline-data-platform](https://github.com/battina1999/airline-data-platform) | Đối soát và kiểm thử mô hình dbt. | Bài toán hàng không khác grain Olist. |
| [nih-clinical-trials-lakehouse-pipeline](https://github.com/mtariqi/nih-clinical-trials-lakehouse-pipeline) | Tổ chức tầng dữ liệu và quality gate cho luồng ingest. | Phạm vi AWS/ML không nằm trong bản lõi. |
| [Vietnam-Air-Quality-Data-Pipeline-Snowflake](https://github.com/dangnha/Vietnam-Air-Quality-Data-Pipeline-Snowflake) | Cách đặt kiểm tra chất lượng và chia mô hình. | Không mang Snowflake vào stack này. |
| [local_lakehouse/manage-lakehouse.sh](https://github.com/vutrinh274/local_lakehouse/blob/main/manage-lakehouse.sh) | Script quản lý vòng đời Compose. | Dùng healthcheck, không dùng fixed sleep hoặc down -v cho dừng thường ngày. |
| [DataLakeHouse](https://github.com/nguyenthanhhungDE/DataLakeHouse) | Tham khảo profiling/nạp Olist và cách chia medallion. | Dự án dùng công nghệ khác; không sao chép schema/pipeline nguyên khối. |
| [ecommerce-analytics-dbt](https://github.com/michaelf108/ecommerce-analytics-dbt) | Tham khảo grain Olist, dbt tests và cách diễn giải chỉ số. | Dùng DuckDB/Power BI; không suy rộng kết luận về retention. |
| [Data-lakehouse](https://github.com/pyoska11/Data-lakehouse#10-service-endpoints) | Tham khảo các điểm nối PostgreSQL CDC → Kafka → Spark → BI. | Dùng Delta/TheLook; kiểm tra lại từng cấu hình với Iceberg/Olist. |
| [minio/openlake](https://github.com/minio/openlake) | Cách kết nối MinIO với Iceberg và bài toán bảo trì bảng. | Bối cảnh Kubernetes khác Compose cục bộ. |
| [real-time-commerce-lakehouse/data-generator](https://github.com/ilyass74/real-time-commerce-lakehouse/tree/main/data-generator) | Chỉ xem xét ý tưởng generator sau khi đọc được mã. | Đường dẫn chưa xác minh được nội dung; chưa dùng làm căn cứ thiết kế. |
| [data-engineering-practice](https://github.com/danielbeach/data-engineering-practice) | Tham khảo bài tập CSV → PostgreSQL và PySpark. | Là tập thực hành, không phải kiến trúc hoàn chỉnh. |

## Tài liệu kỹ thuật gốc

- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Apache Iceberg Spark catalog](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Trino Iceberg/JDBC catalog](https://trino.io/docs/current/object-storage/metastores.html)
- [Spark JDBC data source](https://spark.apache.org/docs/3.5.9/sql-data-sources-jdbc.html)
- [Spark Thrift Server](https://spark.apache.org/docs/3.5.9/sql-distributed-sql-engine.html)
- [dbt-spark: cấu hình Iceberg và incremental](https://docs.getdbt.com/reference/resource-configs/spark-configs)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [dbt incremental strategies](https://docs.getdbt.com/docs/build/incremental-strategy)
- [Docker Compose nhiều tệp](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/)