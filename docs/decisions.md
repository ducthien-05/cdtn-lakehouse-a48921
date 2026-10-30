# Nhật ký quyết định kiến trúc CĐTN

**Trạng thái:** Quyết định thiết kế, cập nhật ngày 29/10/2026. Các lựa chọn dưới đây đã chốt về hướng làm; chưa phải bằng chứng rằng dịch vụ hoặc mã nguồn đã được triển khai thành công.

## 1. Quyết định phạm vi

| ID | Quyết định | Lý do và hệ quả |
|---|---|---|
| D01 | Dự án nằm tại /home/thienduc/cdtn-lakehouse, ngang cấp với bigdata_project. | Musicify là dự án riêng; không nhập mã hoặc thay đổi Musicify khi làm CĐTN. |
| D02 | Dùng 8 CSV Olist: customers, sellers, products, orders, order_items, order_payments, order_reviews, product_category_name_translation. | Thiết kế schema và nạp đủ 8 bảng PostgreSQL trước CDC; đối soát từng tệp. CSV geolocation nằm ngoài phạm vi lõi. |
| D03 | Giữ 10 câu hỏi Q01, Q02, Q04, Q05, Q06, Q08, Q09, Q10, Q11, Q12. Ngừng Q03, Q07, Q13; giữ mã cũ. | Q03 là lát cắt phụ của Q01; Q07 phân mảnh/chồng lặp Q02 và Q05; Q13 khó diễn giải và không đủ cơ sở nhân quả. Gold/KPI/dashboard chỉ phải phục vụ 10 câu hỏi đang dùng. |
| D04 | Không nạp geolocation và không xây postal_locations trong lõi. | Phân tích bang dựa trên state hiện có trong customers/sellers. Thay đổi quyết định này chỉ khi câu hỏi địa lý cụ thể cần tọa độ/zip và đã có quy tắc làm sạch. |
| D05 | Luồng lõi là PostgreSQL → Debezium/Kafka → Spark Structured Streaming → Iceberg Bronze trên MinIO → Trino/dbt tạo Iceberg Silver/Gold → Superset. | Chứng minh CDC, truy vết, chuẩn hóa và phân tích trong một luồng có thể đối soát; dashboard cập nhật sau xử lý. |
| D06 | Chỉ CDC 7 bảng nghiệp vụ: customers, sellers, products, orders, order_items, order_payments, order_reviews. Bảng dịch danh mục nạp tĩnh qua batch từ PostgreSQL sang Iceberg Silver. | Snapshot và thay đổi mô phỏng chỉ dành cho bảng nghiệp vụ; bảng tham chiếu được đối soát khi chạy batch. |
| D07 | Bronze giữ sự kiện CDC kể cả delete; Silver giữ trạng thái hiện tại; Gold giữ fact, dimension và KPI. | Không xóa bằng chứng lịch sử ở Bronze; xử lý trùng/update/delete trước khi tính chỉ số. |
| D08 | Các bảng lưu bền vững ở Bronze, Silver và Gold dùng Apache Iceberg trên MinIO; Spark và Trino dùng cùng một catalog. | Có snapshot và bảng chung cho các engine; model dbt trung gian có thể là view/ephemeral. Kiểm thử tương thích trước khi mở rộng. |
| D09 | Superset đọc Gold sau khi dbt hoàn tất và tests đạt. | Không hứa dashboard thời gian thực; đo độ trễ đến Bronze và Gold riêng. |
| D10 | Airflow, Nessie, Prometheus/Grafana, Great Expectations là phần mở rộng. | Không để hạ tầng mở rộng cản luồng lõi. |
| D11 | Danh sách 10 repo ở architecture.md là nguồn tham khảo chọn lọc. | Áp dụng ý tưởng phù hợp sau khi đọc mã/tài liệu liên quan, kiểm tra phiên bản/giấy phép; không sao chép toàn bộ kiến trúc. |

Các quyết định D02, D03, D04 và D06 cập nhật phạm vi cũ 9 CSV/13 câu hỏi theo yêu cầu ngày 29/10/2026 và câu trả lời mới nhất: “CDC bảng nghiệp vụ, nạp 8 bảng CSV trừ geolocation”.

## 2. P01–P04 đã chốt

| ID | Quyết định ngày 29/10/2026 | Lý do | Việc phải kiểm chứng khi triển khai |
|---|---|---|---|
| P01 | Profiling, thiết kế DDL và nạp đủ 8 bảng PostgreSQL trước; sau đó CDC 7 bảng nghiệp vụ. Dùng orders + order_items → Q01 làm luồng dọc kiểm chứng đầu tiên sau bước nạp đủ nguồn. | Có schema/quan hệ và nguồn đối soát đầy đủ trước khi phát CDC; vẫn giữ một mốc tích hợp nhỏ để tìm lỗi sớm. | Số dòng/khóa/quan hệ của 8 bảng, snapshot 7 bảng, update/delete và Q01 đối soát. |
| P02 | Dùng Iceberg JDBC catalog với PostgreSQL metadata database riêng cho bản lõi; Nessie để sau. | Spark và Trino đều có hỗ trợ JDBC catalog, phù hợp môi trường Compose cục bộ và tránh thêm service REST khi chưa cần. | Tạo metadata tables theo yêu cầu Trino; thử Spark tạo/ghi, Trino đọc/ghi, restart và kiểm tra snapshot. Trino cảnh báo khả năng tương thích JDBC tương lai; nếu kiểm thử không đạt, ghi quyết định mới trước khi đổi REST. |
| P03 | Chạy dbt bằng lệnh/script ở bản lõi; Airflow chỉ thêm sau. | Phù hợp dashboard cập nhật theo lần xử lý; giảm số service ở giai đoạn tích hợp. | Chờ Bronze đến mốc, build/test Silver rồi Gold, ghi log/mã lỗi, chạy lại an toàn và chỉ công bố kết quả sau tests. |
| P04 | Tách Compose thành foundation, CDC, lakehouse, serving nhưng ghép bằng một Compose project cố định. | Dễ bật/kiểm tra theo nhóm mà các dịch vụ vẫn chung mạng và định danh; giảm tệp cấu hình quá lớn. | docker compose config, healthcheck, thứ tự khởi động, đường dẫn tương đối theo tệp base; down thường giữ volume, reset riêng. |

P01–P04 là **quyết định đã chốt**, không còn là “phương án chờ chọn”. Các phép kiểm chứng là điều kiện để khẳng định triển khai hoạt động, không phải điều kiện để hiểu quyết định thiết kế.

## 3. Câu hỏi kỹ thuật còn mở

| ID | Câu hỏi cần giải quyết | Khi nào chốt |
|---|---|---|
| O01 | Kiểu cột, khóa chính/khóa ghép, ràng buộc và thứ tự nạp cho 8 bảng sau profiling cụ thể là gì? Review ID có duy nhất trên dữ liệu thực không; nếu không, khóa ổn định nào dùng cho CDC? | Trước DDL/loader. Phạm vi 8 bảng và 7 bảng CDC đã chốt ở D02/D06. |
| O02 | Converter, topic naming, tombstone, snapshot mode, publication và quyền PostgreSQL cho Debezium cấu hình thế nào? | Trước khi đăng ký connector. |
| O03 | Schema Bronze đầy đủ và quy tắc sắp thứ tự sự kiện cùng khóa là gì? Topic/partition/offset định danh bản ghi Kafka; cần kiểm tra metadata source để phân xử update/delete. | Trước Spark stream và Silver. |
| O04 | Silver xử lý sự kiện lặp, update/delete và eventual consistency ra sao; model nào dùng full build, model nào dùng incremental/merge? | Trước dbt Silver/Gold. |
| O05 | Grain, công thức, bộ lọc và mốc thời gian của 10 câu hỏi đang dùng là gì? Q01 cần chốt giá trị hàng/freight/đơn đã giao; Q09 cần giới hạn cách diễn giải mua lặp lại. | Trước Gold/dashboard tương ứng. |
| O06 | Mốc dữ liệu nhất quán cho đối soát PostgreSQL ↔ Silver và nguồn ↔ Gold xác định thế nào? | Trước chương trình đối soát. |
| O07 | Bộ phiên bản tương thích Spark, Iceberg, Trino, Debezium, Kafka và dbt-trino là gì? | Trước khi khóa image/dependency. |
| O08 | Đưa CSV vào Ubuntu theo cách nào, có lưu bản gốc trong MinIO/raw hay không? | Trước loader. |
| O09 | Ngưỡng nghiệm thu cho độ trễ, thông lượng, idempotency và phục hồi là gì? | Trước thực nghiệm chính thức. |

## 4. Quy tắc cập nhật

- Ghi ngày, lý do, phạm vi ảnh hưởng và phép kiểm chứng khi thay đổi quyết định. Nếu đổi luồng/tầng dữ liệu, cập nhật docs/architecture.md; nếu đổi phạm vi hoặc cách làm việc dài hạn, cập nhật AGENTS.md.
- Docs/business-questions.md và docs/kpi-dictionary.md, khi được tạo, là nơi ghi định nghĩa chi tiết. Không biến bảng quyết định thành tài liệu công thức.
- Không ghi “đã kiểm chứng” nếu chưa có lệnh chạy, kết quả và dữ liệu đối chiếu.
- Giữ danh sách repo và đánh giá mức độ áp dụng tại docs/architecture.md mục 8; đường dẫn generator chưa xác minh nội dung nên chưa dùng làm căn cứ thiết kế.

## Tài liệu kỹ thuật gốc

- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Iceberg catalog trong Spark](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Trino JDBC catalog và lưu ý tương thích](https://trino.io/docs/current/object-storage/metastores.html)
- [Trino PostgreSQL connector](https://trino.io/docs/current/connector/postgresql.html)
- [Docker Compose nhiều tệp](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/)