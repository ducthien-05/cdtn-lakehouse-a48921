# CĐTN Lakehouse — hướng dẫn cho Codex

## Mục tiêu và phạm vi

- Xây dựng lakehouse mã nguồn mở để phân tích thương mại điện tử Olist tại /home/thienduc/cdtn-lakehouse, ngang cấp và độc lập với /home/thienduc/bigdata_project (Musicify).
- Phạm vi lõi nạp 8 CSV Olist vào PostgreSQL: customers, sellers, products, orders, order_items, order_payments, order_reviews và product_category_name_translation. Không nạp geolocation và không xây postal_locations nếu chưa có yêu cầu phân tích địa lý.
- Có 10 câu hỏi kinh doanh đang dùng: Q01, Q02, Q04, Q05, Q06, Q08, Q09, Q10, Q11, Q12. Q03, Q07, Q13 được loại khỏi phạm vi Gold/dashboard; giữ mã Q cũ để dễ truy vết quyết định.
- Luồng lõi: CSV → PostgreSQL → Debezium/Kafka cho 7 bảng nghiệp vụ → Spark Structured Streaming → Iceberg Bronze trên MinIO → Trino/dbt tạo Iceberg Silver và Gold → Superset. Bảng dịch danh mục là bảng tham chiếu nạp tĩnh, đưa vào Silver bằng batch qua Trino/dbt.
- Bronze giữ sự kiện CDC và metadata để truy vết; Silver phản ánh trạng thái hiện tại sau thêm/sửa/xóa; Gold cung cấp fact, dimension và KPI. Chương trình mô phỏng thay đổi PostgreSQL dùng để kiểm chứng CDC.
- Spark và Trino dùng chung một Iceberg JDBC catalog trong bản lõi. Dashboard cập nhật sau khi chạy Silver/Gold; không cam kết thời gian thực. Airflow, Nessie, Prometheus/Grafana và Great Expectations là phần mở rộng.

## Nguồn quyết định và cách làm việc

- Đọc docs/architecture.md và docs/decisions.md trước khi thay đổi kiến trúc; đọc docs/source-schema.md và sql/001_olist_source.sql trước khi sửa bảng nguồn PostgreSQL; khi được tạo, docs/business-questions.md và docs/kpi-dictionary.md lưu câu hỏi, grain và công thức chi tiết. Nếu yêu cầu mới thay đổi quyết định cũ, cập nhật các tài liệu liên quan cùng lúc.
- Danh sách 10 GitHub repo trong docs/architecture.md là nguồn tham khảo chọn lọc. Đọc đúng phần liên quan, kiểm tra phiên bản và giấy phép khi dùng ý tưởng hoặc mã; không sao chép toàn bộ kiến trúc của repo khác.
- Trước CDC, thiết kế schema và loader cho cả 8 bảng PostgreSQL, đối chiếu số dòng/khóa/quan hệ và xử lý lỗi dữ liệu. Chỉ sau đó cấu hình Debezium cho 7 bảng nghiệp vụ; bắt đầu kiểm chứng luồng dọc với orders, order_items và Q01 nhưng không bỏ qua bước nạp đủ 8 bảng.
- Mỗi KPI cần định nghĩa grain, khóa, tập bản ghi hợp lệ, mốc thời gian, nguồn cột, cách xử lý thiếu/trùng và phép đối soát trước khi viết Gold SQL.
- Tách Compose theo nhóm dịch vụ nhưng vận hành bằng cùng tên Compose project và cùng bộ cấu hình; kiểm tra docker compose config và dùng healthcheck. Lệnh dừng thường giữ volume/checkpoint; thao tác xóa dữ liệu phải là lệnh riêng, ghi rõ hậu quả.
- Không commit mật khẩu, token, .env thực tế hoặc CSV nguồn. Cung cấp .env.example. Kiểm chứng thay đổi bằng kết quả chạy thật ở phạm vi phù hợp; không ghi “đã chạy” khi mới có thiết kế.
- Chỉ sửa thư mục dự án CĐTN khi làm việc cho đề tài này; Musicify là dự án tham khảo riêng.