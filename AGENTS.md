# CĐTN Lakehouse — hướng dẫn cho Codex

## Mục tiêu và phạm vi

- Xây dựng lakehouse mã nguồn mở để phân tích thương mại điện tử Olist tại /home/thienduc/cdtn-lakehouse, ngang cấp và độc lập với /home/thienduc/bigdata_project (Musicify).
- Phạm vi lõi nạp 8 CSV Olist vào PostgreSQL: customers, sellers, products, orders, order_items, order_payments, order_reviews và product_category_name_translation. Không nạp geolocation và không xây postal_locations nếu chưa có yêu cầu phân tích địa lý.
- Có 10 câu hỏi kinh doanh đang dùng: Q01, Q02, Q04, Q05, Q06, Q08, Q09, Q10, Q11, Q12. Q03, Q07, Q13 được loại khỏi phạm vi Gold/dashboard; giữ mã Q cũ để dễ truy vết quyết định.
- Luồng lõi chốt lại ngày 13/11/2026: CSV → PostgreSQL → Debezium/Kafka cho 7 bảng nghiệp vụ → Spark Structured Streaming → Iceberg Bronze trên MinIO → dbt qua Spark SQL tạo Iceberg Silver và Gold → Trino phục vụ truy vấn → Superset. Bảng dịch danh mục là tham chiếu tĩnh; Spark batch đọc PostgreSQL bằng JDBC và ghi Iceberg Silver cho model dbt sử dụng.
- Bronze giữ sự kiện CDC và metadata để truy vết; Silver phản ánh trạng thái hiện tại sau thêm/sửa/xóa; Gold cung cấp fact, dimension và KPI. Chương trình mô phỏng thay đổi PostgreSQL dùng để kiểm chứng CDC.
- Spark và Trino dùng chung Iceberg JDBC catalog trong bản lõi. Spark là engine ghi/biến đổi; dbt quản lý model, phụ thuộc và tests qua `dbt-spark`; Trino đọc Gold cho Superset. Streaming Bronze và SQL batch/dbt có vòng đời, tài nguyên và cách chạy lại riêng. SQL endpoint (ưu tiên kiểm chứng Thrift), phiên bản adapter và phân quyền Trino còn cần triển khai; không coi thiết kế là kết quả chạy. Dashboard cập nhật sau Silver/Gold; Airflow, Nessie, Prometheus/Grafana và Great Expectations là mở rộng.

## Nguồn quyết định và cách làm việc

- Trước khi tạo, sửa hoặc xóa bất kỳ file/thư mục nào trong dự án, trình bày đường dẫn, mục đích và nội dung thay đổi dự kiến; chỉ thực hiện sau khi người dùng chốt đồng ý. Không tự chạy git add hoặc git commit; người dùng tự quản lý staging và commit.
- Đọc docs/architecture.md và docs/decisions.md trước khi thay đổi kiến trúc; đọc docs/source-schema.md, docs/data-dictionary.md và sql/001_olist_source.sql trước khi sửa bảng nguồn PostgreSQL hoặc loader; khi được tạo, docs/business-questions.md và docs/kpi-dictionary.md lưu câu hỏi, grain và công thức chi tiết. Nếu yêu cầu mới thay đổi quyết định cũ, cập nhật các tài liệu liên quan cùng lúc.
- Danh sách 10 GitHub repo trong docs/architecture.md là nguồn tham khảo chọn lọc. Đọc đúng phần liên quan, kiểm tra phiên bản và giấy phép khi dùng ý tưởng hoặc mã; không sao chép toàn bộ kiến trúc của repo khác.
- Trước CDC, thiết kế schema và loader cho cả 8 bảng PostgreSQL, đối chiếu số dòng/khóa/quan hệ và xử lý lỗi dữ liệu. Chỉ sau đó cấu hình Debezium cho 7 bảng nghiệp vụ; bắt đầu kiểm chứng luồng dọc với orders, order_items và Q01 nhưng không bỏ qua bước nạp đủ 8 bảng.
- Khi xây Silver, xác định mốc dữ liệu và thứ tự sự kiện theo khóa; full build chọn sự kiện cuối rồi mới lọc delete. Không giả định `merge` mặc định của dbt tự xử lý `op=d`; kiểm chứng delete, sự kiện lặp, chạy lại và kết quả Trino đọc trước khi công nhận đạt.
- Mỗi KPI cần định nghĩa grain, khóa, tập bản ghi hợp lệ, mốc thời gian, nguồn cột, cách xử lý thiếu/trùng và phép đối soát trước khi viết Gold SQL.
- Nghiệm thu CDC theo từng mức: hạ tầng/connector, snapshot đủ số dòng và khóa, so đúng từng cột tại cùng trạng thái nguồn, thử commit `c/u/d` và tombstone, rồi thử phục hồi. Không gọi snapshot chính xác toàn bộ khi mới kiểm tra số dòng và khóa. Phân biệt snapshot JSON cũ không kèm Connect schema với sự kiện mới có `schema`/`payload` khi đọc cùng topic.
- Tách Compose theo nhóm dịch vụ nhưng vận hành bằng cùng tên Compose project và cùng bộ cấu hình; kiểm tra docker compose config và dùng healthcheck. Lệnh dừng thường giữ volume/checkpoint; thao tác xóa dữ liệu phải là lệnh riêng, ghi rõ hậu quả.
- Không commit mật khẩu, token, .env thực tế hoặc CSV nguồn. Cung cấp .env.example. Kiểm chứng thay đổi bằng kết quả chạy thật ở phạm vi phù hợp; không ghi “đã chạy” khi mới có thiết kế.
- Chỉ sửa thư mục dự án CĐTN khi làm việc cho đề tài này; Musicify là dự án tham khảo riêng.
