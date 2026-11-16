# Từ điển dữ liệu nguồn Olist

**Phạm vi:** 8 bảng CSV trong schema PostgreSQL olist, không gồm geolocation. Tài liệu mô tả nguồn để thiết kế loader, CDC và Silver/Gold. Profiling ngày 30/10/2026; DDL/loader đã chạy và 8 bảng được đối soát ngày 01/11 theo `docs/source-schema.md`. Ngày 13/11 chốt Spark/dbt biến đổi Silver/Gold, Trino phục vụ truy vấn; chưa có kết quả nghiệm thu các model.

**Nguồn sự thật:** cấu trúc và ràng buộc ở sql/001_olist_source.sql; số liệu quan sát ở data/olist_profile.json (không commit); lý do chọn khóa và kiểu ở docs/source-schema.md. Cột “Ô trống CSV” là số ô rỗng trong bản CSV đang dùng, không phải số NULL đã đo trong PostgreSQL. Khi các nguồn này khác nhau, kiểm tra lại CSV và cập nhật tài liệu cùng DDL.

## Quy ước chung

- Mỗi bảng có grain là một bản ghi theo khóa chính nêu bên dưới. Tên bảng/cột PostgreSQL giữ giống CSV để nạp và đối soát trực tiếp.
- ID Olist là chuỗi varchar(32); không chuyển sang số. ZIP prefix là chuỗi đúng 5 chữ số để giữ số 0 đầu. State là mã bang 2 ký tự; city là text.
- Timestamp nguồn không có múi giờ; PostgreSQL dùng timestamp without time zone. Không tự diễn giải thành UTC.
- Tiền là numeric(12,2); kích thước, cân nặng, số lượng và điểm review dùng integer. Ràng buộc CHECK nằm trong DDL.
- “Cho NULL” là quy tắc DDL. Ô CSV trống khi nạp sẽ thành NULL; giá trị 0 và chuỗi rỗng có ý nghĩa khác nhau.
- Bảy bảng nghiệp vụ được CDC: customers, sellers, products, orders, order_items, order_payments, order_reviews. Spark streaming giữ sự kiện vào Bronze; dbt qua Spark SQL chuẩn hóa và áp dụng CDC vào Silver, tạo Gold. product_category_name_translation được Spark batch đọc PostgreSQL qua JDBC và ghi tham chiếu Iceberg Silver.
- Số dòng dưới đây là dữ liệu lịch sử ban đầu. Generator và CDC sẽ làm số dòng thay đổi; không dùng các số này làm hằng số trong pipeline.

## Tổng quan bảng

| Bảng olist | CSV | Grain / khóa chính | Dòng CSV | Vai trò |
|---|---|---|---:|---|
| product_category_name_translation | product_category_name_translation.csv | 1 tên danh mục tiếng Bồ Đào Nha / product_category_name | 71 | Tra cứu tĩnh, batch |
| customers | olist_customers_dataset.csv | 1 customer_id / customer_id | 99.441 | Khách gắn với đơn hàng |
| sellers | olist_sellers_dataset.csv | 1 seller_id / seller_id | 3.095 | Người bán |
| products | olist_products_dataset.csv | 1 product_id / product_id | 32.951 | Sản phẩm |
| orders | olist_orders_dataset.csv | 1 order_id / order_id | 99.441 | Đơn hàng và vòng đời giao |
| order_items | olist_order_items_dataset.csv | 1 dòng hàng trong đơn / (order_id, order_item_id) | 112.650 | Sản phẩm, seller, giá và phí vận chuyển theo dòng |
| order_payments | olist_order_payments_dataset.csv | 1 lần/nhánh thanh toán trong đơn / (order_id, payment_sequential) | 103.886 | Phương thức và giá trị thanh toán |
| order_reviews | olist_order_reviews_dataset.csv | 1 review gắn với đơn / (review_id, order_id) | 99.224 | Điểm và nội dung đánh giá |

## product_category_name_translation

| Cột | Kiểu PostgreSQL | Cho NULL | Ô trống CSV | Ý nghĩa |
|---|---|---|---:|---|
| product_category_name | varchar(100) | Không | 0 | Tên danh mục gốc; khóa chính. |
| product_category_name_english | varchar(100) | Có | 0 | Tên tiếng Anh để hiển thị/phân tích. |

Bảng này chỉ có 71 danh mục. products có 610 dòng không có danh mục và 13 dòng thuộc 2 tên danh mục chưa có bản dịch. Vì vậy products không có FK cứng đến bảng dịch; Silver/Gold cần LEFT JOIN và cách hiển thị danh mục chưa dịch.

## customers

| Cột | Kiểu PostgreSQL | Cho NULL | Ô trống CSV | Ý nghĩa |
|---|---|---|---:|---|
| customer_id | varchar(32) | Không | 0 | Khóa bản ghi khách gắn với một order; khóa chính. |
| customer_unique_id | varchar(32) | Không | 0 | ID nhận diện người mua qua nhiều customer_id; không UNIQUE. |
| customer_zip_code_prefix | varchar(5) | Không | 0 | 5 chữ số đầu mã bưu chính của khách. |
| customer_city | text | Không | 0 | Thành phố của khách theo nguồn. |
| customer_state | varchar(2) | Không | 0 | Mã bang của khách. |

Không suy ra số người mua duy nhất từ số dòng customers; dùng customer_unique_id với quy tắc KPI tương ứng.

## sellers

| Cột | Kiểu PostgreSQL | Cho NULL | Ô trống CSV | Ý nghĩa |
|---|---|---|---:|---|
| seller_id | varchar(32) | Không | 0 | ID người bán; khóa chính. |
| seller_zip_code_prefix | varchar(5) | Không | 0 | 5 chữ số đầu mã bưu chính của seller. |
| seller_city | text | Không | 0 | Thành phố của seller; dài tới 40 ký tự trong CSV hiện tại. |
| seller_state | varchar(2) | Không | 0 | Mã bang của seller. |

## products

| Cột | Kiểu PostgreSQL | Cho NULL | Ô trống CSV | Ý nghĩa |
|---|---|---|---:|---|
| product_id | varchar(32) | Không | 0 | ID sản phẩm; khóa chính. |
| product_category_name | varchar(100) | Có | 610 | Tên danh mục gốc; tra cứu mềm với bảng dịch. |
| product_name_lenght | integer | Có | 610 | Độ dài tên sản phẩm; giữ chính tả CSV. |
| product_description_lenght | integer | Có | 610 | Độ dài mô tả; giữ chính tả CSV. |
| product_photos_qty | integer | Có | 610 | Số ảnh sản phẩm. |
| product_weight_g | integer | Có | 2 | Cân nặng, gram. |
| product_length_cm | integer | Có | 2 | Chiều dài, cm. |
| product_height_cm | integer | Có | 2 | Chiều cao, cm. |
| product_width_cm | integer | Có | 2 | Chiều rộng, cm. |

Đổi tên hai cột lenght thành length ở Silver, không đổi header nguồn. Các giá trị kích thước/cân nặng không âm theo DDL.

## orders

| Cột | Kiểu PostgreSQL | Cho NULL | Ô trống CSV | Ý nghĩa |
|---|---|---|---:|---|
| order_id | varchar(32) | Không | 0 | ID đơn hàng; khóa chính. |
| customer_id | varchar(32) | Không | 0 | FK đến customers.customer_id. |
| order_status | varchar(20) | Không | 0 | Trạng thái đơn theo nguồn; không ép PostgreSQL enum. |
| order_purchase_timestamp | timestamp without time zone | Không | 0 | Thời điểm đặt hàng. |
| order_approved_at | timestamp without time zone | Có | 160 | Thời điểm duyệt/thanh toán được xác nhận. |
| order_delivered_carrier_date | timestamp without time zone | Có | 1.783 | Thời điểm bàn giao cho đơn vị vận chuyển. |
| order_delivered_customer_date | timestamp without time zone | Có | 2.965 | Thời điểm giao cho khách. |
| order_estimated_delivery_date | timestamp without time zone | Không | 0 | Ngày giao dự kiến theo nguồn. |

Không mặc định các đơn đều delivered. KPI về giao hàng phải chọn trạng thái, mốc thời gian và xử lý timestamp thiếu trong docs/kpi-dictionary.md.

## order_items

| Cột | Kiểu PostgreSQL | Cho NULL | Ô trống CSV | Ý nghĩa |
|---|---|---|---:|---|
| order_id | varchar(32) | Không | 0 | FK đến orders.order_id; một phần khóa chính. |
| order_item_id | integer | Không | 0 | Số thứ tự dòng trong order, bắt đầu từ 1; một phần khóa chính. |
| product_id | varchar(32) | Không | 0 | FK đến products.product_id. |
| seller_id | varchar(32) | Không | 0 | FK đến sellers.seller_id. |
| shipping_limit_date | timestamp without time zone | Không | 0 | Hạn gửi hàng của dòng item. |
| price | numeric(12,2) | Không | 0 | Giá của dòng item, chưa cộng dồn theo order. |
| freight_value | numeric(12,2) | Không | 0 | Phí vận chuyển của dòng item. |

Tổng price hoặc freight_value theo order phải tổng hợp các dòng item; phép tính doanh thu cần quy tắc KPI rõ ràng, tránh nhân tiền khi JOIN với payments/reviews.

## order_payments

| Cột | Kiểu PostgreSQL | Cho NULL | Ô trống CSV | Ý nghĩa |
|---|---|---|---:|---|
| order_id | varchar(32) | Không | 0 | FK đến orders.order_id; một phần khóa chính. |
| payment_sequential | integer | Không | 0 | Số thứ tự thanh toán trong order, bắt đầu từ 1; một phần khóa chính. |
| payment_type | varchar(30) | Không | 0 | Phương thức thanh toán. |
| payment_installments | integer | Không | 0 | Số kỳ trả góp trong bản ghi; nguồn có giá trị 0. |
| payment_value | numeric(12,2) | Không | 0 | Giá trị của bản ghi thanh toán. |

Một order có thể có nhiều payment; muốn tính tổng thanh toán thì SUM theo order trước khi JOIN với item/review.

## order_reviews

| Cột | Kiểu PostgreSQL | Cho NULL | Ô trống CSV | Ý nghĩa |
|---|---|---|---:|---|
| review_id | varchar(32) | Không | 0 | ID đánh giá; riêng cột này không duy nhất trong CSV. |
| order_id | varchar(32) | Không | 0 | FK đến orders.order_id; cùng review_id tạo khóa chính. |
| review_score | integer | Không | 0 | Điểm đánh giá, CHECK từ 1 đến 5. |
| review_comment_title | text | Có | 87.658 | Tiêu đề nhận xét; thường trống. |
| review_comment_message | text | Có | 58.274 | Nội dung nhận xét; thường trống. |
| review_creation_date | timestamp without time zone | Không | 0 | Mốc tạo review theo nguồn. |
| review_answer_timestamp | timestamp without time zone | Có | 0 | Mốc trả lời review; nullable để hỗ trợ thay đổi sau này. |

review_id lặp ở 814 dòng, còn cặp (review_id, order_id) không trùng trong CSV. Không coi order_id là duy nhất trong reviews; khi tính KPI theo đơn cần nêu rõ cách xử lý nhiều review.

## Trách nhiệm dữ liệu sau nguồn

Spark Structured Streaming ghi Bronze với envelope và metadata truy vết. dbt quản lý model/phụ thuộc/tests, còn Spark SQL thực thi chuẩn hóa kiểu, tên cột và trạng thái Silver theo khóa nguồn; Gold áp dụng grain/công thức KPI đã chốt. Trino đọc Gold cho Superset. Thay engine xử lý không thay định nghĩa cột/khóa nguồn ở tài liệu này.

Khi đối soát Silver với PostgreSQL, chọn cùng mốc thay đổi đã xử lý. Delete phải phản ánh ở trạng thái hiện tại; sự kiện delete/tombstone vẫn được giữ ở Bronze. Không dùng số dòng CSV cố định để kết luận Silver đúng sau các thay đổi nguồn.

## Quan hệ, nạp và cập nhật

- Nạp theo thứ tự: product_category_name_translation, customers, sellers, products, orders, order_items, order_payments, order_reviews. Các FK cứng trong DDL có 0 dòng mồ côi ở CSV đã profiling.
- Loader đã nạp và đối soát 8 bảng theo `docs/source-schema.md`; tiếp tục dùng profile làm mốc CSV. Cột “Ô trống CSV” trong tài liệu này là thống kê nguồn cố định, không tự trở thành phép kiểm tra PostgreSQL hoặc Silver hiện tại.
- Generator sau này dùng khóa tổng hợp của chính nó, tạo parent trước child, update trạng thái/timestamp có kiểm soát, xóa child trước parent. Với reviews, dùng cả review_id và order_id để định danh. Không dựa vào giả định một order chỉ có một item, payment hay review.
- Khi thay đổi cột nguồn, cập nhật DDL, profiler, loader và từ điển này. Khi định nghĩa chỉ số kinh doanh, cập nhật docs/kpi-dictionary.md; từ điển này mô tả cột nguồn, không thay thế định nghĩa KPI.

