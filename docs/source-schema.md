# PostgreSQL source schema từ 8 CSV Olist

**Trạng thái:** DDL đã thiết kế từ 8 CSV thật và báo cáo data/olist_profile.json ngày 30/10/2026; chưa chạy trên PostgreSQL. File thực thi: sql/001_olist_source.sql. Dữ liệu nguồn và JSON profiling nằm trong data/ và không được commit.

## Cơ sở thiết kế

Nguồn là [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) trên Kaggle. Thống kê Data Explorer trong ảnh người dùng cung cấp (khoảng 99,4 nghìn customers và 99,2 nghìn reviews, review score 1–5, comment thường thiếu) phù hợp về quy mô với CSV địa phương. Các số chính xác sau đây lấy từ CSV địa phương, vì DDL phải áp dụng được cho chính bản tải về này.

| Bảng PostgreSQL trong schema olist | Số dòng CSV | Khóa chính | Điểm ảnh hưởng DDL |
|---|---:|---|---|
| product_category_name_translation | 71 | product_category_name | Bản dịch tĩnh; có 2 tên danh mục trong products không có bản dịch. |
| customers | 99.441 | customer_id | customer_unique_id được phép lặp để nhận diện khách mua lại; ZIP là chuỗi 5 chữ số. |
| sellers | 3.095 | seller_id | seller_city dài tới 40 ký tự, vượt varchar(32) trong sơ đồ ban đầu. |
| products | 32.951 | product_id | 610 dòng thiếu danh mục/tên mô tả/số ảnh; 2 dòng thiếu cân nặng/kích thước. |
| orders | 99.441 | order_id | 160 thiếu approved_at, 1.783 thiếu carrier_date, 2.965 thiếu delivered_customer_date. |
| order_items | 112.650 | (order_id, order_item_id) | Không trùng; price/freight có tối đa 2 chữ số thập phân. |
| order_payments | 103.886 | (order_id, payment_sequential) | Không trùng; payment_installments có giá trị 0. |
| order_reviews | 99.224 | (review_id, order_id) | review_id một mình lặp 814 dòng; cặp khóa không trùng. Comment title/message thường rỗng. |

Các FK customers → orders, orders → order_items/payments/reviews, products/sellers → order_items đều có **0 dòng mồ côi** trong CSV địa phương. Products có **13 dòng** thuộc hai danh mục không có trong bảng dịch: pc_gamer (3) và portateis_cozinha_e_preparadores_de_alimentos (10). Bảng dịch vẫn giữ đúng 71 dòng CSV; không tạo khóa ngoại cứng từ products.product_category_name sang bảng dịch. Ở Silver/Gold, dùng LEFT JOIN và định nghĩa cách hiển thị tên chưa dịch.

Giá trị số tiền lớn nhất là 13.664,08 ở payment_value; cả price, freight_value và payment_value có tối đa 2 chữ số thập phân. numeric(12,2) có dư địa cho dữ liệu generator. Timestamps trong CSV không có múi giờ nên dùng timestamp without time zone; không giả định chúng đã được chuyển sang UTC. ZIP được giữ dạng varchar(5) để không mất số 0 đầu.

## Điều chỉnh so với sơ đồ dbdiagram.io

- Giữ **tên bảng và cột giống CSV** ở PostgreSQL nguồn để nạp/đối soát trực tiếp: order_payments thay vì payments, order_reviews thay vì reviews, và hai cột product_name_lenght/product_description_lenght giữ đúng chính tả gốc. Tên được sửa ở Silver.
- Không dùng product_category_id tự tăng ở bảng nguồn. Bảng dịch là product_category_name_translation với khóa tự nhiên; products giữ product_category_name nullable. Nếu cần dimension surrogate cho BI, tạo ở Gold. Cách này giữ 8 CSV ↔ 8 bảng và không phải tạo thêm hàng danh mục giả để che 13 dòng chưa dịch.
- Giữ customer_unique_id không UNIQUE. seller_city dùng text vì độ dài thực tế 40 ký tự; city của generator có thể dài hơn.
- Khóa order_reviews là (review_id, order_id), phù hợp báo cáo profiling. order_id riêng cũng không UNIQUE vì một đơn có thể có nhiều review.
- review_answer_timestamp cho phép NULL để generator có thể tạo review trước rồi thêm thời điểm trả lời sau; CSV lịch sử hiện không thiếu trường này.
- payment_installments cho phép 0; các số tiền và kích thước dùng CHECK không âm. order_status không dùng PostgreSQL enum để generator có thể thử trạng thái mới có kiểm soát sau này; những KPI lõi phải lọc giá trị thực tế theo từ điển KPI.

## Nạp dữ liệu và đối soát ở bước kế tiếp

1. Khởi động PostgreSQL nguồn riêng của CĐTN, tạo database và áp DDL một lần trên database mới.
2. Nạp theo thứ tự: bảng dịch danh mục, customers, sellers, products, orders, order_items, order_payments, order_reviews.
3. Dùng COPY/psql \copy với CSV HEADER và quy tắc chuyển ô trống thành NULL; loader phải dừng khi lỗi, ghi số dòng và chỉ báo thành công khi 8 bảng đối soát đúng các số trên.
4. Chạy lại loader trên database đã có phải có chiến lược rõ ràng (staging/transaction hoặc reset có chủ đích), không âm thầm nhân đôi.
5. Sau khi nạp đủ 8 bảng mới tạo Debezium snapshot cho 7 bảng nghiệp vụ; bảng dịch danh mục đi theo batch.

DDL không có ON DELETE CASCADE. Generator sau này chỉ thao tác trên các ID tổng hợp của chính nó: tạo parent trước child; cập nhật order_status/timestamps, review hoặc payment; khi thử delete thì xóa child trước parent. Product mới chọn danh mục đã biết hoặc để NULL; không dựa vào thay đổi bảng dịch qua CDC. Các kịch bản này là hướng thiết kế, chưa có mã generator.

## Kiểm chứng hiện tại

Ba test của scripts/profile_olist.py với fixture nhỏ đạt, và báo cáo profiling trên 8 CSV địa phương có complete=true, 0 lỗi header/kiểu được ghi nhận. File JSON ban đầu bị hỏng cú pháp ở phần customers đã được sao lưu thành data/olist_profile.corrupt.json và tạo lại từ CSV. DDL chưa được chạy trên PostgreSQL do môi trường hiện chưa có psql hoặc image PostgreSQL; kết quả thực thi và đối soát DB sẽ được ghi sau.