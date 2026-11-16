# Profiling nguồn Olist trước khi thiết kế PostgreSQL

**Trạng thái:** Bộ 8 CSV được chốt cố định; đã profiling ngày 30/10/2026. Báo cáo chuẩn là data/olist_profile.json. Kết quả DDL và quyết định khóa được ghi trong docs/source-schema.md. Quyết định D02/D06 yêu cầu nạp 8 CSV vào PostgreSQL và CDC 7 bảng nghiệp vụ; geolocation nằm ngoài phạm vi lõi.

## Nguồn và phạm vi

Nguồn chính là [Brazilian E-Commerce Public Dataset by Olist trên Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), gồm 9 CSV và được công bố theo giấy phép CC BY-NC-SA 4.0. Không đưa tệp gốc vào Git. Tám tệp cần dùng:

| Bảng dự kiến | Tên tệp CSV |
|---|---|
| product_category_name_translation | product_category_name_translation.csv |
| customers | olist_customers_dataset.csv |
| sellers | olist_sellers_dataset.csv |
| products | olist_products_dataset.csv |
| orders | olist_orders_dataset.csv |
| order_items | olist_order_items_dataset.csv |
| order_payments | olist_order_payments_dataset.csv |
| order_reviews | olist_order_reviews_dataset.csv |

Đặt các tệp trong data/raw/ của dự án hoặc truyền một thư mục khác qua --data-dir. Script chỉ đọc tám tên tệp trên; olist_geolocation_dataset.csv không được xử lý.

## Báo cáo chuẩn và kiểm tra lại khi cần

data/olist_profile.json là mốc của 8 CSV cố định tại data/raw/. Báo cáo lưu SHA-256 từng file, header, số dòng, số ô trống, độ dài text tối đa, độ chính xác số thập phân, giá trị sai kiểu, khoảng số/thời gian, khóa ứng viên và quan hệ. File này ở trong data/ nên không được Git theo dõi. Không tự ghi đè báo cáo chuẩn khi kiểm tra ingest.

Giữ scripts/profile_olist.py để kiểm tra lại CSV khi nghi ngờ file bị thay đổi; không cần chạy sau mỗi lần ingest. Nếu cần tạo báo cáo mới, ghi ra một đường dẫn tạm rồi so với mốc chuẩn:

~~~bash
python3 scripts/profile_olist.py --data-dir data/raw --output /tmp/olist_profile_check.json
~~~

Script chỉ đọc CSV, không kết nối PostgreSQL. Mã thoát 2 báo thiếu file hoặc header sai. Trường complete=true chỉ nghĩa là đủ file và header hợp lệ; cần xem riêng các chỉ số sai kiểu, dòng lỗi, khóa trùng và quan hệ. ZIP prefix được đọc như văn bản để giữ số 0 đầu. review_id có thể lặp, còn cặp (review_id, order_id) là khóa đã chọn trong DDL.

## Đối soát sau khi nạp PostgreSQL

1. Trước ingest, xác nhận SHA-256 của 8 CSV vẫn khớp data/olist_profile.json.
2. Sau ingest, dùng truy vấn PostgreSQL so số dòng và số NULL theo cột với báo cáo chuẩn; kiểm tra khóa, FK và các min/max quan trọng. data-dictionary.md giải thích ý nghĩa cột, không tự thực hiện phép kiểm tra.
3. Quan hệ products.product_category_name với bảng dịch có 13 dòng chưa dịch trong CSV gốc; đây là ngoại lệ đã ghi ở docs/source-schema.md, không phải lỗi ingest.
4. Mốc JSON áp dụng cho dữ liệu CSV ban đầu. Sau khi generator thay đổi PostgreSQL, đối soát PostgreSQL với các tầng lakehouse theo cùng mốc dữ liệu, không so số dòng động với JSON cố định.
5. Chỉ bật CDC cho 7 bảng nghiệp vụ sau khi 8 bảng PostgreSQL đã được nạp và đối soát.
6. Theo phân vai chốt ngày 13/11/2026, Spark streaming đưa CDC vào Iceberg Bronze; dbt qua Spark SQL tạo Silver/Gold; Trino phục vụ truy vấn. Đối soát tiếp theo là PostgreSQL ↔ Silver tại cùng mốc đã xử lý và Gold ↔ phép tính độc lập; profile CSV không thay thế các kiểm tra này.
7. Bảng dịch danh mục đi theo Spark batch JDBC từ PostgreSQL sang Iceberg Silver. Kiểm tra khóa, số dòng và nội dung với nguồn tham chiếu ở thời điểm batch; 71 dòng là mốc CSV ban đầu, không mã hóa thành hằng số khi nguồn được thay đổi có chủ đích.

Không coi số dòng trong README của repo tham khảo là bằng chứng cho CSV thực tế của dự án này.
