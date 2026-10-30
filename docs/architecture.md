# Kiến trúc hệ thống CĐTN Lakehouse

**Trạng thái:** Thiết kế đã thống nhất về phạm vi và hướng triển khai, cập nhật ngày 29/10/2026. Chưa xác nhận dịch vụ hoặc pipeline đã chạy thành công.

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
    K --> SP["Spark Structured Streaming"]
    SP --> B[("Iceberg Bronze: CDC events")]
    B --> TR["Trino + dbt"]
    PG -->|bảng dịch danh mục: batch qua Trino PostgreSQL connector| TR
    TR --> S[("Iceberg Silver: trạng thái chuẩn hóa")]
    S --> TR
    TR --> G[("Iceberg Gold: fact, dimension, KPI")]
    G --> BI["Superset"]
~~~

Bảy bảng CDC là customers, sellers, products, orders, order_items, order_payments và order_reviews. Bảng product_category_name_translation là tham chiếu tĩnh: nạp vào PostgreSQL, sau đó dbt đọc qua Trino PostgreSQL connector và materialize thành bảng tham chiếu ở Iceberg Silver. Nếu bảng này cần thay đổi trong tương lai, chạy lại bước batch có đối soát; không giả định nó tự cập nhật qua CDC.

MinIO lưu data/metadata files của Iceberg Bronze, Silver và Gold. Một Iceberg JDBC catalog dùng chung cho Spark và Trino lưu tham chiếu metadata/snapshot; nên tách database/schema catalog khỏi các bảng nghiệp vụ PostgreSQL để tránh lẫn trách nhiệm. PostgreSQL nguồn là hệ thống mô phỏng giao dịch, không dùng làm kho phân tích. Trino đọc và ghi bảng Iceberg cho dbt; Superset chỉ đọc Gold.

## 3. Trách nhiệm từng tầng và định dạng

| Thành phần | Đầu vào → đầu ra | Trách nhiệm |
|---|---|---|
| Loader | 8 CSV → 8 bảng PostgreSQL | Xác thực cột/kiểu, thiết kế khóa sau profiling, nạp có thể chạy lại, ghi số dòng hợp lệ/lỗi và đối soát quan hệ. |
| PostgreSQL nguồn | Dữ liệu lịch sử + thay đổi mô phỏng | Giữ bảng nghiệp vụ có khóa ổn định; bật logical replication cho đúng 7 bảng CDC. |
| Debezium + Kafka | PostgreSQL WAL → topic theo bảng | Snapshot ban đầu cho 7 bảng, sau đó phát insert/update/delete đã commit; bảng dịch danh mục không nằm trong table include list. |
| Spark Structured Streaming | Kafka → Iceberg Bronze | Parse envelope, ghi append-only và lưu topic/partition/offset, bảng, khóa, thao tác, metadata nguồn và thời điểm ingest. |
| Iceberg Bronze | Sự kiện CDC | Lưu dấu vết thay đổi, kể cả delete; không dùng trực tiếp để tính KPI trạng thái hiện tại. |
| dbt/Trino Silver | Bronze + bảng dịch danh mục → Iceberg Silver | Chuẩn hóa kiểu, loại trùng sự kiện, áp dụng update/delete và tạo bảng trạng thái hiện tại theo khóa nghiệp vụ. |
| dbt/Trino Gold | Silver → Iceberg Gold | Xây fact/dimension và KPI cho 10 câu hỏi, với grain/bộ lọc/thời gian xác định. |
| Superset | Gold → dashboard | Hiển thị kết quả sau mỗi lần chạy dbt thành công; không cam kết thời gian thực. |

Iceberg áp dụng cho **các bảng được lưu bền vững ở cả ba tầng** để Spark/Trino cùng đọc được, quản lý snapshot và hỗ trợ xây lại/đối soát. Không bắt mọi model dbt trung gian thành bảng: model chỉ dùng để tổ chức SQL có thể là view hoặc ephemeral. Silver và Gold là các bảng Iceberg materialized; chiến lược dbt table/full refresh hay incremental/merge sẽ chọn theo từng model sau thử nghiệm với phiên bản cố định. Với Olist cỡ nhỏ, ưu tiên cách xây lại dễ kiểm chứng trước; sau đó đo chi phí và thử incremental khi cần xử lý CDC hiệu quả. Xóa ở Silver phải được áp dụng vào trạng thái hiện tại, đồng thời Bronze vẫn giữ sự kiện xóa.

## 4. Chất lượng, nhất quán và phục hồi

- Trước khi áp ràng buộc PostgreSQL, profiling từng CSV để xác nhận khóa dự kiến, đặc biệt khóa ghép của order_items/order_payments và tính duy nhất của review_id. Nếu dữ liệu thật vi phạm, ghi quy tắc xử lý hoặc khóa thay thế ổn định; không âm thầm bỏ dòng.
- Loader đối chiếu số dòng nguồn, số dòng nạp, dòng lỗi, khóa trùng và quan hệ giữa 8 bảng. Giữ báo cáo nạp theo từng lần chạy.
- Bronze nhận diện bản ghi Kafka bằng topic + partition + offset; kiểm tra metadata Debezium phục vụ sắp thứ tự thay đổi trên từng khóa. Silver phải chống sự kiện lặp và xử lý delete; không giả định exactly-once xuyên toàn bộ pipeline.
- dbt tests kiểm tra unique/not_null/relationships/accepted values và quy tắc nghiệp vụ ở Silver/Gold. Đối soát PostgreSQL với Silver sau khi CDC bắt kịp một mốc đã chọn; đối chiếu Gold bằng truy vấn độc lập ở cùng mốc.
- Thử restart Spark/Kafka, tiếp tục từ checkpoint, chạy lại dbt và kiểm tra kết quả không nhân đôi. Đo độ trễ PostgreSQL → Bronze và PostgreSQL → Gold riêng.
- Ghi kịch bản, dữ liệu và ngưỡng nghiệm thu vào docs/experiments.md trước khi tuyên bố pipeline đạt yêu cầu.

## 5. Hạ tầng và điều phối

Tách Compose thành foundation (PostgreSQL nguồn, MinIO, JDBC catalog database), CDC (Kafka, Kafka Connect/Debezium), lakehouse (Spark, Trino) và serving (Superset, dbt runner). Ghép các tệp dưới **một Compose project cố định** để dùng chung mạng, service name và dependency. Kiểm tra cấu hình ghép bằng docker compose config; đường dẫn tương đối của Compose cần thống nhất với tệp base. Dùng healthcheck thay vì chờ bằng thời gian cố định. Script quản lý có thể bọc lệnh config/up/ps/logs/down và kiểm tra sẵn sàng; lệnh down thường không xóa volume, reset dữ liệu là thao tác riêng.

Trong bản lõi, chạy dbt bằng lệnh hoặc script có trình tự: kiểm tra Bronze đã đến mốc dữ liệu, build Silver → test → build Gold → test → cho dashboard sử dụng. Ghi mã thoát và log; không báo thành công khi một bước lỗi. Airflow, Nessie, Prometheus/Grafana và Great Expectations chỉ thêm sau khi luồng lõi chạy và đã có bằng chứng kiểm chứng.

## 6. Thứ tự triển khai và mốc kiểm chứng

1. Profiling cả 8 CSV: cột, kiểu, khóa ứng viên, quan hệ, null, trùng và phạm vi thời gian. Thiết kế DDL PostgreSQL cho **đủ 8 bảng**, thứ tự nạp theo phụ thuộc và chính sách xử lý lỗi; không tạo bảng geolocation.
2. Viết loader, nạp đủ 8 bảng vào PostgreSQL và đối soát từng tệp. Kiểm tra chạy lại không nhân đôi. Tạo bản sao/mốc nguồn để phân biệt dữ liệu gốc với thay đổi mô phỏng.
3. Bật logical replication và đăng ký Debezium cho 7 bảng nghiệp vụ. Kiểm tra snapshot của cả 7 bảng, topic/khóa/số dòng, rồi mô phỏng insert/update/delete an toàn theo quan hệ; bảng dịch danh mục đi theo batch.
4. Cấu hình MinIO và Iceberg JDBC catalog dùng chung; thử Spark ghi một bảng Iceberg, Trino đọc/ghi cùng bảng và kiểm tra sau restart. Sau đó ghi Bronze CDC từ Kafka với checkpoint và metadata.
5. Xây Silver cho đủ bảng cần phục vụ 10 câu hỏi, gồm bảng dịch danh mục nạp batch; kiểm tra trạng thái sau update/delete, unique key và đối soát với PostgreSQL.
6. Xây Gold và dashboard theo thứ tự giá trị: Q01 làm luồng dọc kiểm chứng đầu tiên sau khi đã nạp đủ nguồn, rồi mở rộng đến 9 câu hỏi còn lại. Chốt KPI dictionary và chạy data tests/đối soát cho từng nhóm.
7. Chạy thực nghiệm phục hồi, độ trễ và chạy lại; chỉ thêm Airflow hoặc thành phần mở rộng nếu có nhu cầu đã đo được.

## 7. Lựa chọn kỹ thuật và phần cần kiểm chứng

| Vấn đề | Hướng đã chọn | Phép kiểm chứng còn cần |
|---|---|---|
| Phạm vi nguồn/CDC | Nạp 8 bảng PostgreSQL; CDC 7 bảng nghiệp vụ; bảng dịch danh mục batch; bỏ geolocation. | Profiling khóa, DDL, quyền publication và snapshot đủ bảng. |
| Iceberg | Bảng bền vững Bronze/Silver/Gold dùng Iceberg trên MinIO, cùng JDBC catalog. | Spark ↔ Trino tạo/đọc/ghi, restart và metadata JDBC; chuẩn bị metadata tables theo yêu cầu Trino. Nếu vướng tương thích, ghi ADR mới trước khi đổi sang REST. |
| Bronze | Append-only envelope CDC và metadata truy vết; topic/partition/offset là định danh bản ghi Kafka. | Kiểu dữ liệu Debezium thực tế, tombstone/delete, snapshot và thứ tự thay đổi trên từng khóa. |
| Silver/Gold | dbt qua Trino tạo bảng Iceberg; bước đầu full build có đối soát, sau đó thử incremental/merge theo model. | Trino Iceberg DML, dbt-trino materialization, xử lý delete và chi phí chạy lại với bộ phiên bản đã khóa. |
| Điều phối | Script/lệnh dbt trước; Airflow là mở rộng. | Mốc dữ liệu, chạy lỗi, chạy lại và phát hành dashboard sau tests. |
| Compose | Các file theo nhóm, một project, healthcheck và reset riêng. | docker compose config, thứ tự khởi động và bảo toàn volume. |

Chi tiết chưa chốt gồm schema cột/khóa sau profiling, phiên bản tương thích, converter và topic naming Debezium, quy tắc ordering trong Bronze/Silver, grain/công thức KPI và ngưỡng nghiệm thu. Các mục này được theo dõi trong docs/decisions.md; không ghi là đã kiểm chứng khi chưa chạy thử.

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
- [Trino PostgreSQL connector](https://trino.io/docs/current/connector/postgresql.html)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [dbt incremental strategies](https://docs.getdbt.com/docs/build/incremental-strategy)
- [Docker Compose nhiều tệp](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/)