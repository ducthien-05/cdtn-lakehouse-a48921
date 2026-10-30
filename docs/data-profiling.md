# Profiling nguồn Olist trước khi thiết kế PostgreSQL

**Trạng thái:** Đã chạy trên 8 CSV thật ngày 30/10/2026; báo cáo hợp lệ tại data/olist_profile.json. Kết quả DDL và quyết định khóa được ghi trong docs/source-schema.md. Quyết định D02/D06 yêu cầu nạp 8 CSV vào PostgreSQL và CDC 7 bảng nghiệp vụ; geolocation nằm ngoài phạm vi lõi.

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

## Chạy profiling

Từ /home/thienduc/cdtn-lakehouse:

~~~bash
python3 scripts/profile_olist.py --data-dir data/raw --output data/olist_profile.json
~~~

Nếu CSV ở nơi khác, thay data/raw bằng đường dẫn thư mục đó. File báo cáo JSON nằm trong data/ (đã ignore trong Git), gồm SHA-256 của từng nguồn, header, số dòng, số ô trống, độ dài text tối đa, precision/scale số thập phân, số giá trị không đúng loại dự kiến, khoảng giá trị số/thời gian, số dòng trùng cho khóa ứng viên và số dòng không khớp khóa ngoại. Lệnh trả mã 2 khi thiếu tệp hoặc header sai; báo cáo vẫn được ghi để xem lỗi. Trường complete chỉ có nghĩa đủ tệp và header hợp lệ, **không** có nghĩa dữ liệu đã đạt mọi quy tắc chất lượng.

Các kiểu cột và khóa trong script là **giả thuyết cần kiểm chứng**. Đặc biệt, review_id có thể không duy nhất; script kiểm tra cả review_id và cặp review_id + order_id, không âm thầm bỏ dòng. Zip code prefix được đọc như văn bản để không mất số 0 đầu.

## Điều kiện để chốt DDL và loader

1. Có báo cáo từ đúng phiên bản CSV thực tế, lưu SHA-256 để truy vết.
2. Xác nhận header và kiểu cho cả tám tệp; ghi rõ cách xử lý ô trống và giá trị sai kiểu.
3. Chọn khóa chính/khóa thay thế ổn định từ kết quả trùng và thiếu; kiểm tra các quan hệ customer, order, product, seller và category.
4. Thiết kế DDL đủ tám bảng và thứ tự nạp; loader phải đối soát số dòng và chạy lại mà không nhân đôi.
5. Chỉ sau khi nạp và đối soát PostgreSQL mới bật CDC cho bảy bảng nghiệp vụ.

Không coi số dòng trong README của repo tham khảo là bằng chứng cho CSV thực tế của dự án này.