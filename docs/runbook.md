# Runbook vận hành và kiểm chứng CĐTN Lakehouse

**Mốc cập nhật 15/11/2026:** PostgreSQL đã nạp 8 CSV; CDC snapshot 550.688 sự kiện đã đối soát từng cột ngày 08/11, thử c/u/d/tombstone và khôi phục nguồn. Người dùng đã báo thử phục hồi CDC và thao tác bảng Iceberg qua Spark/Trino trong chat; còn lưu bằng chứng. DDL/job/checker/service Bronze đã viết và kiểm tra offline; chưa áp DDL hoặc chạy ingest dữ liệu thật trong lần triển khai này. Các kết quả ghi “Kỳ vọng” ở mục 5 cần được người vận hành chạy/đối chiếu. SQL endpoint/dbt, Silver/Gold và dashboard chưa triển khai.

## 1. Phạm vi và cách dùng runbook

Chạy **từng khối lệnh riêng** trong Ubuntu, từ `/home/thienduc/cdtn-lakehouse`. Các lệnh Compose luôn ghép foundation → CDC → lakehouse theo cùng project `cdtn-lakehouse`; lệnh `up` chỉ khởi động service được nêu và các phụ thuộc của chúng. Không in toàn bộ `docker compose config` vì nó có thể chứa mật khẩu trong `.env`.

| Phần | Mục đích | Trạng thái |
|---|---|---|
| PostgreSQL nguồn | 8 bảng từ 8 CSV; `product_category_name_translation` là bảng tham chiếu batch | Đã nạp/đối soát ngày 01/11 |
| CDC/Kafka | Snapshot 7 bảng và phát thay đổi đã commit vào 7 topic `olist_pg.olist.<table>` | Snapshot từng cột và `c/u/d` đã đạt; phục hồi đã được người dùng thực nghiệm, còn lưu bằng chứng |
| Lakehouse | MinIO/catalog dùng chung; Spark ghi Iceberg, Trino phục vụ truy vấn | Đã khởi động/đọc catalog ở phiên trước; còn kiểm chứng bảng ghi thật, xem mục 4 |
| Bronze | Spark streaming → 7 bảng Iceberg sự kiện, một checkpoint mỗi bảng | DDL/job/checker/service đã viết, chưa chạy với dữ liệu thật; xem mục 5 |
| Silver/Gold/dashboard | dbt qua Spark SQL → Silver/Gold; Superset đọc qua Trino | dbt project/SQL endpoint và dashboard chưa triển khai |

Phân vai đã chốt ngày 13/11 ở D16: dbt biên dịch model và gửi SQL cho Spark; Trino đọc Gold qua cùng catalog/MinIO. Spark batch JDBC nạp bảng dịch danh mục từ PostgreSQL thành tham chiếu Iceberg Silver. Chạy dbt là một đợt xử lý riêng với streaming Bronze. Runbook này ghi vận hành các thành phần hiện có; thử nghiệm kết nối/model dbt-spark mới được hướng dẫn trong phiên chat, chưa đưa thành thủ tục vận hành.

Dữ liệu nguồn ở volume `cdtn-lakehouse_postgres_source_data`; Kafka ở `cdtn-lakehouse_kafka_data` gắn vào `/kafka/data`. Connect giữ cấu hình, offset và trạng thái trong ba topic nội bộ `olist_connect_configs`, `olist_connect_offsets`, `olist_connect_statuses`. Kafka giữ log sự kiện, không phải bảng trạng thái hiện tại; retention broker hiện là 720 giờ (30 ngày). Không xóa volume, slot hoặc topic để chạy lại snapshot. Bảng dịch danh mục không có topic CDC.

**Hai định dạng JSON cùng tồn tại:** snapshot ban đầu được phát khi `schemas.enable=false`, nên `op` nằm ở `.op`. Ngày 08/11 connector và template đã chuyển sang `key.converter.schemas.enable=true` và `value.converter.schemas.enable=true`; sự kiện mới có `schema` và `payload`, nên `op` nằm ở `.payload.op`. Việc đổi converter không sửa các message cũ và không tự tạo Schema Registry. Parser Bronze hiện đã xử lý cả hai dạng; cần nghiệm thu với dữ liệu thật theo mục 5.

## 2. PostgreSQL nguồn + CDC + Kafka

### 2.1. Sau khi mở máy lại: chạy hằng ngày

Đảm bảo Docker Desktop đã chạy và Ubuntu truy cập được Docker. Vào dự án và dùng venv hiện có:

```bash
cd /home/thienduc/cdtn-lakehouse
```

```bash
source venv-cdtn/bin/activate
```

```bash
docker info --format '{{.ServerVersion}}'
```

Kỳ vọng: in phiên bản Docker Server. Nếu lỗi kết nối, khởi động Docker Desktop rồi thử lại.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml up -d --wait --wait-timeout 180 postgres-source kafka connect
```

Kỳ vọng: `postgres-source`, `kafka`, `connect` chạy nền và healthy. `connect` đợi PostgreSQL/Kafka healthy theo `depends_on`; `--wait` đợi healthcheck trước khi trả về. Lệnh có thể chạy lại khi ba container đã hoạt động.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml ps postgres-source kafka connect
```

Kỳ vọng: cả ba dòng là `Up ... (healthy)`. Nếu vừa khởi động mà chưa healthy, đợi rồi kiểm tra lại.

```bash
python scripts/check_olist_cdc.py check
```

Kỳ vọng: `wal_level=logical`, publication `olist_cdc_pub` đúng 7 bảng, slot `olist_cdc_slot` dùng `pgoutput` và `active=True`, connector `olist-postgres-cdc` cùng một task `RUNNING`, đúng 7 topic Olist. Lệnh chỉ đọc nguồn/Connect/Kafka. Không cần cài lại Python packages, chạy lại loader, SQL tạo publication, `register` hoặc đọc 550.688 snapshot event sau mỗi lần mở máy. File template không tự cập nhật connector đã đăng ký; cấu hình chạy được giữ trong Connect.

### 2.2. Chỉ dùng khi thiết lập trên database/volume mới

**Không chạy mục này trên dữ liệu hiện tại.** Các lệnh DDL và `load` dưới đây dành cho PostgreSQL nguồn rỗng; loader từ chối nạp chồng. Bảo đảm đủ 8 CSV trong `data/raw/` và `.env` có thông tin PostgreSQL/MinIO trước khi bắt đầu. Nguồn và DDL chi tiết ở [source-schema.md](source-schema.md).

```bash
source venv-cdtn/bin/activate
```

```bash
python -m pip install -r requirements-loader.txt
```

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml config --quiet
```

```bash
python scripts/olist_cdc.py validate
```

Kỳ vọng: hai lệnh kiểm tra không báo lỗi; `validate` in đúng tên connector, publication, slot và 7 bảng, không in mật khẩu.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml up -d --wait postgres-source
```

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec -T postgres-source sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/001_olist_source.sql
```

Kỳ vọng: DDL tạo schema/bảng trên database rỗng. Chỉ áp một lần.

```bash
python scripts/load_olist.py check
```

```bash
python scripts/load_olist.py load
```

Kỳ vọng: `check` xác nhận 8 CSV khớp profile; `load` nạp và đối soát 550.759 dòng của 8 bảng rồi commit. `load` ghi PostgreSQL, nên chỉ chạy ở lần thiết lập mới.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml up -d --wait kafka connect
```

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec -T postgres-source sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/002_olist_cdc.sql
```

Kỳ vọng: tạo publication đúng 7 bảng. Không chạy lại SQL nếu publication đã tồn tại.

```bash
python scripts/check_olist_cdc.py preflight
```

Kỳ vọng: PostgreSQL logical replication/quyền/publication và 7 số dòng nguồn khớp CSV; plugin PostgreSQL có trong Connect. Slot chưa tạo ở bước này là bình thường.

```bash
python scripts/olist_cdc.py register
```

```bash
python scripts/olist_cdc.py status
```

Kỳ vọng: `register` tạo connector một lần và bắt đầu snapshot; sau khi ổn định, connector/task là `RUNNING`. `register` không cập nhật connector đã tồn tại. Với template hiện tại, snapshot mới sẽ kèm Connect schema; snapshot lịch sử trong hệ thống đang chạy được phát trước lúc bật schema.

### 2.3. Xem lỗi và dừng giữ dữ liệu

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml logs --tail=100 postgres-source kafka connect
```

Dùng khi `up`, `ps` hoặc `check` báo lỗi. Nếu connector mất sau restart, kiểm tra volume Kafka và ba topic nội bộ trước khi tính chuyện đăng ký lại; không xóa slot/offset để ép snapshot.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml stop connect kafka postgres-source
```

Nếu `spark-bronze` đang chạy, dừng job theo mục 5.8 trước khi dừng Kafka. Lệnh `stop` dừng ba service nhưng giữ named volumes. Không dùng `down -v` cho dừng thường ngày. Nếu chỉ dừng Connect mà PostgreSQL vẫn ghi dữ liệu, slot có thể giữ WAL; theo dõi dung lượng trước khi để lâu.

## 3. Kiểm chứng và đối soát CDC/Kafka

### 3.1. Kiểm kê topic, offset và dung lượng

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec -T kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Kỳ vọng: 7 topic `olist_pg.olist.*`; topic `olist_connect_*` và `__consumer_offsets` là nội bộ. Không có topic của bảng dịch danh mục.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec -T kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic 'olist_pg.olist.*'
```

Kỳ vọng: mỗi topic có một partition, replication factor 1; partition 0 có leader và ISR là broker 1. Đây là cấu hình local một broker, không có bản sao dự phòng.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec -T kafka /kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic 'olist_pg.olist.*' --time -2
```

`--time -2` là earliest offset; ngày 08/11 cả 7 topic có earliest 0. Snapshot offset 0 chỉ đọc lại được khi các segment ban đầu còn được Kafka giữ.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec -T kafka /kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic 'olist_pg.olist.*' --time -1
```

`--time -1` là latest offset, vị trí ngay sau bản ghi cuối. Sau thử `c/u/d` trên customers, latest của topic này là 99.445: 99.441 snapshot + `c`, `u`, `d` và một tombstone. Các topic còn lại vẫn ở mốc snapshot:

| Bảng/topic cuối | Snapshot `op=r` | Latest offset 08/11 |
|---|---:|---:|
| customers | 99.441 | 99.445 |
| sellers | 3.095 | 3.095 |
| products | 32.951 | 32.951 |
| orders | 99.441 | 99.441 |
| order_items | 112.650 | 112.650 |
| order_payments | 103.886 | 103.886 |
| order_reviews | 99.224 | 99.224 |
| **Tổng snapshot** | **550.688** | |

Offset là vị trí, không phải số dòng nguồn hay bộ đếm sự kiện đáng tin trong mọi tình huống. Retention, compaction và các thay đổi mới làm phép trừ offset không còn là đối soát snapshot.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec -T kafka /kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka:9092 --describe --topic-list olist_pg.olist.customers,olist_pg.olist.sellers,olist_pg.olist.products,olist_pg.olist.orders,olist_pg.olist.order_items,olist_pg.olist.order_payments,olist_pg.olist.order_reviews | jq -Rr 'fromjson? | .brokers[].logDirs[].partitions[] | [.partition, .size, ((.size / 1048576 * 100 | round) / 100)] | @tsv'
```

Ba cột là `topic-partition`, byte và MiB. `jq -Rr 'fromjson?'` bỏ qua hai dòng thông báo trước JSON của CLI. Đã đo ngày 08/11: 7 topic dùng 530.664.502 byte (506,08 MiB) log segment; số này thay đổi theo sự kiện và retention. Nó không phải toàn bộ dung lượng volume Kafka, vì volume còn topic nội bộ và file khác.

### 3.2. Đối soát snapshot ban đầu: từ số dòng đến từng cột

```bash
python scripts/check_olist_cdc.py snapshot
```

Lệnh chỉ đọc 550.688 event đầu, yêu cầu `op=r`, đếm theo bảng và kiểm Kafka key khớp PK trong `after`. **Chỉ bước này chưa chứng minh giá trị mọi cột đúng.** Đã đạt ngày 08/11.

```bash
python scripts/check_olist_cdc.py snapshot-fields
```

Lệnh mới chỉ đọc PostgreSQL và 7 topic, cố định một view PostgreSQL `REPEATABLE READ, READ ONLY`, rồi so snapshot từ offset 0 với nguồn theo PK và **mọi cột**. Nó nhận cả JSON cũ lẫn `schema`/`payload`, chuẩn hóa `NUMERIC` bằng `Decimal`, timestamp PostgreSQL không múi giờ thành microsecond từ mốc 1970-01-01, giữ phân biệt `NULL` và chuỗi rỗng. Nó báo `source`, `snapshot`, `distinct_pk`, `missing`, `extra`, `duplicate`, `key_mismatch`, `column_mismatch`, `row_mismatch`, `field_mismatch` cho từng bảng và trả mã lỗi khi có lệch.

Kết quả chạy thật ngày 08/11: đủ 550.688 dòng của 7 bảng, mọi số lệch trên đều **0**; dòng cuối là `Snapshot fields reconciled: 550688 rows across 7 topic(s)`. Có thể thử riêng một bảng bằng `python scripts/check_olist_cdc.py snapshot-fields --table order_items` (đã đạt với 112.650 dòng).

Phép so này dùng **trạng thái PostgreSQL tại lúc chạy**, còn Kafka `op=r` phản ánh thời điểm snapshot ban đầu. Chỉ dùng kết quả như bằng chứng snapshot ↔ nguồn cùng nội dung khi biết nguồn chưa thay đổi, hoặc các thay đổi thử đã được khôi phục; bộ khách hàng tổng hợp ở mục 3.4 đã được xóa. Nếu sau này nguồn thay đổi thật, lệch có thể là thay đổi hợp lệ sau snapshot: so với CSV gốc đã chuẩn hóa theo loader hoặc backup PostgreSQL đúng thời điểm snapshot trước khi quy lỗi cho CDC. Dữ liệu Kafka ban đầu còn phải chưa hết retention. Lệnh không ghi bảng nguồn, topic hay file báo cáo.

Để xem một message snapshot cũ, đọc đúng offset 0 của topic `orders`:

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec -T kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic olist_pg.olist.orders --partition 0 --offset 0 --max-messages 1 --timeout-ms 10000 | jq -C .
```

Kỳ vọng: JSON `op=r` nằm ở mức gốc vì message này có từ trước khi bật Connect schema. Đây là phép xem mẫu, không thay thế đối soát 550.688 dòng.

### 3.3. Xem sự kiện mới tự động trong terminal

Mở một terminal riêng và để lệnh sau chạy liên tục. Nó bắt đầu ở cuối topic, chỉ in các sự kiện mới `c/u/d`, không cần tự tìm offset. `jq` bản Ubuntu hiện có là 1.8.1; nếu thiếu, cài `jq` bằng trình quản lý gói của Ubuntu trước.

```bash
docker compose --env-file .env \
  -f infra/compose.foundation.yml \
  -f infra/compose.cdc.yml \
  -f infra/compose.lakehouse.yml \
  exec -T kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --include 'olist_pg\.olist\.(customers|sellers|products|orders|order_items|order_payments|order_reviews)' \
| jq -C --unbuffered 'select((.payload.op // .op) | IN("c", "u", "d"))'
```

Terminal có thể trống khi chưa có commit mới. Thông báo về KIP-848 của Kafka consumer không phải lỗi; khi thực hiện `INSERT`, `UPDATE`, `DELETE` ở terminal khác và `COMMIT`, JSON sẽ xuất hiện. `.payload.op // .op` đọc được cả hai định dạng; `jq` vẫn in toàn bộ JSON, kể cả `schema` nếu có. Dừng theo dõi bằng `Ctrl+C`. Tombstone có value `null` sẽ bị bộ lọc trên ẩn; muốn nhìn cả tombstone, bỏ phần `select(...)` và dùng `| jq -C --unbuffered .`.

### 3.4. Thử `c/u/d` với bản ghi tổng hợp và khôi phục nguồn

Kịch bản dưới đây đã chạy ngày 08/11 với ID `cdc_demo_20261108_001`; trước thử ID vắng mặt và `customers=99.441`, sau thử ID lại vắng mặt và số dòng vẫn 99.441. Có thể chạy lại chỉ khi xác nhận ID hiện không tồn tại. Chỉ thao tác trên bản ghi tổng hợp, không sửa dòng Olist gốc. **Mỗi câu SQL chạy riêng trong psql với autocommit mặc định; không đặt ba câu trong một `BEGIN` chung.** Mở terminal xem live ở mục 3.3 trước.

Mở psql nguồn trong terminal thứ hai:

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec postgres-source sh -c 'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

**Trước `c`:** xác nhận mốc và sao lưu trạng thái ban đầu là *không có ID thử*:

```sql
SELECT count(*) FROM olist.customers;
SELECT * FROM olist.customers WHERE customer_id = 'cdc_demo_20261108_001';
```

Kỳ vọng `99.441` và 0 dòng. Nếu ID đã tồn tại, dừng và kiểm tra; không ghi đè. Sau đó tạo một dòng:

```sql
INSERT INTO olist.customers
  (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
VALUES
  ('cdc_demo_20261108_001', 'cdc_demo_unique_20261108_001', '01000', 'cdc_before', 'SP');
```

Kỳ vọng terminal Kafka có `.payload.op="c"`, `before=null`, `after.customer_id` là ID thử, `after.customer_city="cdc_before"`. **Khôi phục nếu dừng ngay sau bước `c`:** `DELETE FROM olist.customers WHERE customer_id = 'cdc_demo_20261108_001';` (sẽ phát thêm `d`/tombstone).

**Trước `u`:** đọc đủ 5 cột làm mốc sao lưu; dự kiến city là `cdc_before`:

```sql
SELECT * FROM olist.customers WHERE customer_id = 'cdc_demo_20261108_001';
```

```sql
UPDATE olist.customers
SET customer_city = 'cdc_after'
WHERE customer_id = 'cdc_demo_20261108_001';
```

Kỳ vọng `.payload.op="u"`, `after.customer_city="cdc_after"`. Với `REPLICA IDENTITY DEFAULT`, `before` của update này là `null`; không dùng `before` làm backup. **Khôi phục riêng bước `u` nếu dừng tại đây:** `UPDATE olist.customers SET customer_city = 'cdc_before' WHERE customer_id = 'cdc_demo_20261108_001';` (sẽ phát thêm `u`). Nếu tiếp tục sang `d`, không chạy câu khôi phục này.

**Trước `d`:** đọc lại đủ 5 cột; bản sao dự phòng đã quan sát là `('cdc_demo_20261108_001', 'cdc_demo_unique_20261108_001', '01000', 'cdc_after', 'SP')`. Bảo đảm không có `orders` tham chiếu ID thử:

```sql
SELECT * FROM olist.customers WHERE customer_id = 'cdc_demo_20261108_001';
SELECT count(*) FROM olist.orders WHERE customer_id = 'cdc_demo_20261108_001';
```

Kỳ vọng có đúng một khách hàng và `orders=0`. Xóa dòng thử:

```sql
DELETE FROM olist.customers WHERE customer_id = 'cdc_demo_20261108_001';
```

Kỳ vọng `.payload.op="d"`, `after=null`, rồi một tombstone value `null` (ẩn bởi bộ lọc mục 3.3). `before` của delete chỉ đáng tin cho PK; các cột khác có thể là placeholder chuỗi rỗng vì `REPLICA IDENTITY DEFAULT`. **Nếu muốn hoàn tác riêng thao tác xóa để trở lại trạng thái ngay trước `d`**, chạy câu sau; nó sẽ phát thêm `c`:

```sql
INSERT INTO olist.customers
  (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
VALUES
  ('cdc_demo_20261108_001', 'cdc_demo_unique_20261108_001', '01000', 'cdc_after', 'SP');
```

Trong chuỗi thử đủ `c→u→d`, **không chạy câu khôi phục riêng này** vì trạng thái nguồn ban đầu là ID vắng mặt.

Đối soát cuối:

```sql
SELECT count(*) FROM olist.customers;
SELECT * FROM olist.customers WHERE customer_id = 'cdc_demo_20261108_001';
```

Kỳ vọng `99.441` và 0 dòng. Gõ `\q` để thoát psql. Lần chạy đã đối chiếu độc lập Kafka: `c`, `u`, `d` và tombstone ở offsets 99.441–99.444 của topic `customers`. Các sự kiện thử vẫn tồn tại trong Kafka cho tới khi retention dọn; khôi phục nguồn không xóa lịch sử CDC.

### 3.5. Mức nghiệm thu và phần còn chờ

| Phép nghiệm thu | Điều kiện đạt | Bằng chứng đến 13/11/2026 |
|---|---|---|
| Hạ tầng CDC | Ba service healthy; publication 7 bảng, slot active, connector/task RUNNING và đủ 7 topic | **Đạt** |
| Snapshot số dòng/khóa | 550.688 `op=r`, key Kafka khớp PK trong `after` | **Đạt** |
| Snapshot từng cột | 7 bảng: PK thiếu/thừa/trùng = 0; cột thiếu/thừa = 0; dòng/cột lệch giá trị = 0 so với PostgreSQL tại view chỉ đọc | **Đạt** với trạng thái nguồn đã khôi phục |
| Thay đổi liên tục | Commit `c/u/d`, đúng topic/ID/nội dung; tombstone sau delete, nguồn về mốc ban đầu | **Đạt** với một khách hàng tổng hợp |
| Phục hồi CDC | Restart giữ volume/slot/offset; sự kiện mới vẫn đến, không snapshot lại ngoài ý muốn; ghi offset và kiểm tra sự kiện lặp nếu có | Người dùng đã thực nghiệm; **chưa lưu offset/log để đóng hồ sơ** |
| Lakehouse | Spark ghi Iceberg, Trino đọc cùng bảng; metadata/dữ liệu giữ qua restart | Đã khởi động và đọc catalog/namespace ở phiên trước; **còn đối soát bảng và phục hồi**, xem mục 4 |

Trước khi đóng nghiệm thu CDC, lưu bằng chứng offset/log cho phép phục hồi người dùng đã thực nghiệm. Với lakehouse, tiếp tục kiểm tra Spark ghi → Trino đọc. Khi xây Bronze, lưu topic/partition/offset, key, envelope, thao tác, thời gian và checkpoint; parser chấp nhận cả hai dạng JSON đã nêu. Model Silver/Gold sau đó chạy qua dbt-spark.

### Tài liệu kỹ thuật gốc

- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/3.7/connectors/postgresql.html)
- [PostgreSQL CREATE PUBLICATION](https://www.postgresql.org/docs/17/sql-createpublication.html)
- [Kafka log directory size](https://kafka.apache.org/protocol.html)
- [Docker Compose `up --wait`](https://docs.docker.com/reference/cli/docker/compose/up)

## 4. Lakehouse: MinIO → Iceberg catalog → Spark/Trino

### 4.1. Thành phần và địa chỉ

| Dịch vụ | Vai trò | Địa chỉ từ máy host |
|---|---|---|
| `postgres-catalog` | Cơ sở dữ liệu metadata Iceberg JDBC V1, tách khỏi PostgreSQL nguồn | Chỉ dùng nội bộ Compose |
| `minio` | Lưu data/metadata Iceberg trên bucket `olist-lakehouse` | API `http://localhost:9000`; Console `http://localhost:9001` |
| `minio-init` | Tạo bucket nếu chưa có, chạy một lần rồi thoát mã 0 | Không mở cổng |
| `spark` | Runner local 2 core để áp DDL, chạy checker và SQL; SQL endpoint dbt chưa có | Chỉ dùng nội bộ Compose |
| `spark-bronze` | Service opt-in chạy job streaming riêng, một query/checkpoint mỗi bảng được chọn | Không mở cổng; xem log/progress/health theo mục 5 |
| `trino` | Query engine dùng chung catalog `lakehouse`; đích phục vụ Superset đọc Gold | `http://localhost:8080` |

Credentials đã có trong `.env` local và không đưa vào báo cáo/output. MinIO server dùng image Chainguard khóa digest, UID 0 để ghi named volume; `minio-init` hiện dùng `cgr.dev/chainguard/minio-client:latest-dev` có `/bin/sh`, chưa khóa digest. Spark 3.5.9, Iceberg 1.12.0, PostgreSQL JDBC 42.7.14 và Trino 484 đã chọn theo phiên bản. Spark tải các JAR từ Maven lần đầu và dùng `spark.jars.ivy /tmp/ivy-cache`; cache giữ qua restart cùng container, có thể mất khi recreate vì chưa có volume cache.

Tất cả lệnh bên dưới chạy **từng lệnh một trong Ubuntu**, từ thư mục dự án. Các lệnh kiểm tra bảng thử sẽ ghi **chỉ vào bucket và PostgreSQL catalog mới**, không sửa 8 bảng Olist nguồn. Không chạy các lệnh smoke test nếu bạn chỉ muốn xem dịch vụ khởi động.

### 4.2. Kiểm tra cấu hình

```bash
cd /home/thienduc/cdtn-lakehouse
```

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml config --quiet
```

Kỳ vọng: mã thoát 0, không có output. Lệnh `config --quiet` tránh in mật khẩu đã nội suy. File foundation phải đứng đầu trong chuỗi `-f` để đường dẫn tương đối trong Compose được giải quyết từ thư mục `infra/`.

### 4.3. Khởi động riêng tầng lakehouse

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml up -d postgres-catalog minio minio-init spark trino
```

Kỳ vọng: `postgres-catalog` và `minio` healthy; `minio-init` hoàn tất thành công, sau đó `spark` và `trino` healthy. Lệnh này không chỉ định `kafka` hoặc `connect`; CDC vẫn là bước kiểm chứng riêng. Nếu image Chainguard yêu cầu đăng nhập registry hoặc không pull được, dừng tại đây và xem thông báo Docker; đừng đổi image không có kiểm chứng.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml ps --all
```

Kỳ vọng: `minio-init` có trạng thái `Exited (0)`; các dịch vụ còn lại có trạng thái `healthy`. Healthcheck `spark` xác nhận file cấu hình trong container đã sẵn sàng, không có nghĩa là job Spark đã chạy thành công.

### 4.4. Đối soát metadata và catalog

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec postgres-catalog psql -U iceberg -d iceberg_catalog -c "\dt iceberg*"
```

Kỳ vọng: đúng hai bảng `iceberg_tables` và `iceberg_namespace_properties`. SQL khởi tạo trong `sql/003_iceberg_catalog.sql` chỉ tự chạy khi volume `postgres_catalog_data` còn mới.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec trino trino --execute "SHOW CATALOGS"
```

Kỳ vọng: có catalog `lakehouse` (cùng với catalog mặc định của Trino). Lệnh này chỉ kiểm tra Trino nạp cấu hình, chưa chứng minh ghi được Iceberg.

### 4.5. Smoke test Spark ghi → Trino đọc

Các lệnh sau dùng riêng bảng `lakehouse.bronze.lakehouse_smoke`. `CREATE OR REPLACE` làm mới **bảng thử này** mỗi lần kiểm tra. Phép kiểm tra sẵn có được đồng bộ theo phân vai Spark ghi, Trino đọc; nó chưa kiểm chứng dbt hoặc luồng Kafka → Bronze.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec spark /opt/spark/bin/spark-sql -e "CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze"
```

Kỳ vọng: tạo namespace `bronze`; lệnh chạy lại không lỗi.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec spark /opt/spark/bin/spark-sql -e "CREATE OR REPLACE TABLE lakehouse.bronze.lakehouse_smoke USING iceberg AS SELECT CAST(1 AS BIGINT) AS id, 'spark' AS writer"
```

Kỳ vọng: Spark tạo bảng Iceberg và ghi một dòng. MinIO có object dưới `olist-lakehouse/warehouse`; PostgreSQL catalog có con trỏ metadata của bảng.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec trino trino --execute "SELECT id, writer FROM lakehouse.bronze.lakehouse_smoke ORDER BY id"
```

Kỳ vọng: dòng `1,spark`.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec spark /opt/spark/bin/spark-sql -e "INSERT INTO lakehouse.bronze.lakehouse_smoke VALUES (2, 'spark')"
```

Kỳ vọng: Spark ghi thêm một dòng vào chính bảng thử.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec trino trino --execute "SELECT id, writer FROM lakehouse.bronze.lakehouse_smoke ORDER BY id"
```

Kỳ vọng: Trino đọc được hai dòng, `1,spark` và `2,spark`. Spark đã ghi và Trino đọc qua cùng catalog; đây là kiểm tra tương thích bảng, không phải CDC hoặc dbt.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml exec postgres-catalog psql -U iceberg -d iceberg_catalog -c "SELECT catalog_name, table_namespace, table_name, iceberg_type FROM iceberg_tables WHERE table_namespace = 'bronze' AND table_name = 'lakehouse_smoke';"
```

Kỳ vọng: một dòng `lakehouse | bronze | lakehouse_smoke | TABLE`.

### 4.6. Thử phục hồi và dừng giữ dữ liệu

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml restart spark trino
```

Sau khi Trino healthy trở lại, chạy lại câu `SELECT` ở mục 4.5. Kỳ vọng vẫn có hai dòng. Điều này kiểm tra dữ liệu và metadata được giữ qua restart của engine.

```bash
docker compose --env-file .env -f infra/compose.foundation.yml -f infra/compose.cdc.yml -f infra/compose.lakehouse.yml stop spark trino minio postgres-catalog
```

Lệnh `stop` giữ `minio_data` và `postgres_catalog_data`; PostgreSQL nguồn và các dịch vụ CDC đang chạy (nếu có) không nằm trong danh sách dừng. Không dùng `down -v` cho dừng thường ngày.

### 4.7. Giới hạn hiện tại

- Đã khởi động các service và truy vấn catalog/namespace ở phiên trước; chưa có kết quả đầy đủ cho bảng thử và phục hồi ở mục 4.5–4.6. Các output ghi “Kỳ vọng” vẫn cần đối chiếu với lần chạy thực tế.
- Runner `spark` chờ bằng `sleep infinity`; healthcheck chỉ xác nhận file cấu hình. Job/checkpoint Bronze chạy ở service riêng `spark-bronze`, đã có code nhưng chưa khởi chạy trong lần triển khai này. SQL endpoint cho dbt còn chờ.
- Catalog này dùng JDBC V1. Trino yêu cầu hai bảng metadata tồn tại trước khi nạp catalog; Spark và Trino phải cùng dùng tên `lakehouse` và warehouse `s3://olist-lakehouse/warehouse`.
- Nếu gặp lỗi, xem riêng `docker compose ... logs minio postgres-catalog minio-init spark trino`; không in bản `docker compose config` đầy đủ vì nó chứa mật khẩu.

### 4.8. Tài liệu tham khảo

- [Apache Iceberg JDBC catalog](https://iceberg.apache.org/docs/latest/jdbc/)
- [Apache Iceberg Spark catalog](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [dbt-spark: cấu hình model Iceberg](https://docs.getdbt.com/reference/resource-configs/spark-configs)
- [Trino JDBC catalog](https://trino.io/docs/current/object-storage/metastores.html)
- [Trino S3/MinIO](https://trino.io/docs/current/object-storage/file-system-s3.html)
- [Chainguard MinIO image](https://www.chainguard.dev/docs/chainguard/containers/getting-started/web-and-data-services/minio)

## 5. Kafka → Spark Structured Streaming → Iceberg Bronze

### 5.1. Phạm vi, trạng thái và schema

**Mục này là quy trình vận hành cho code đã viết, chưa phải biên bản chạy thành công.** Bước tạo DDL và bật service có ghi PostgreSQL catalog/MinIO; chưa được chạy trong lần triển khai code. Các bước `validate`, `bounds`, `status`, `verify` chỉ kiểm tra hoặc đọc. Không chạy lại loader, đăng ký lại connector hay xóa offset để thiết lập Bronze.

Mỗi topic `olist_pg.olist.<table>` đi vào `lakehouse.bronze.<table>`. DDL tạo đủ 7 bảng customers, sellers, products, orders, order_items, order_payments, order_reviews; `.env` mặc định chỉ chọn `orders,order_items`. Bảng dịch danh mục không thuộc Bronze CDC.

| Cột | Kiểu Spark SQL | Ý nghĩa |
| --- | --- | --- |
| `kafka_topic` | STRING | Topic nguồn. |
| `kafka_partition` | INT | Partition nguồn; miền thứ tự của offset. |
| `kafka_offset` | BIGINT | Offset sự kiện, cùng topic/partition tạo identity logic. |
| `kafka_timestamp` | TIMESTAMP | Timestamp Kafka, hiển thị theo UTC trong job. |
| `kafka_timestamp_type` | INT | Loại timestamp do Kafka connector trả về. |
| `raw_key` | BINARY | Key gốc đúng byte. |
| `raw_value` | BINARY | Value gốc đúng byte; NULL cho Kafka tombstone. |
| `key_json` | STRING | Key đã bỏ Connect wrapper, JSON chuẩn hóa thứ tự key. |
| `key_has_schema` | BOOLEAN | Key gốc có wrapper schema/payload. |
| `value_has_schema` | BOOLEAN | Value gốc có wrapper schema/payload; false với tombstone. |
| `op` | STRING | Debezium r/c/u/d; NULL với tombstone hoặc lỗi parse. |
| `source_db` | STRING | Database được Debezium ghi nhận. |
| `source_schema` | STRING | Schema nguồn, dự kiến olist. |
| `source_table` | STRING | Bảng nguồn. |
| `source_lsn` | BIGINT | LSN do Debezium cung cấp, giữ chính xác int64. |
| `source_tx_id` | BIGINT | Transaction ID nguồn nếu có. |
| `source_ts_ms` | BIGINT | Timestamp nguồn milliseconds nếu có. |
| `event_ts_ms` | BIGINT | Timestamp envelope Debezium milliseconds nếu có. |
| `source_snapshot` | STRING | Cờ snapshot chuẩn hóa, ví dụ true/false/last. |
| `before_json` | STRING | before dưới dạng JSON; không coi là backup đầy đủ khi REPLICA IDENTITY DEFAULT. |
| `after_json` | STRING | after dưới dạng JSON; NULL ở delete/tombstone. |
| `is_tombstone` | BOOLEAN | Kafka value NULL; khác bytes của chuỗi JSON null. |
| `parse_error` | STRING | Các mã lỗi envelope/key/source; message vẫn được giữ để xử lý. |
| `ingested_at` | TIMESTAMP | Thời điểm Spark ingest theo UTC. |

DDL không khai báo UNIQUE/PK giả; checker kiểm tra identity, NULL và trùng. Các kiểu nghiệp vụ trong before/after còn giữ dạng JSON, gồm timestamp số và NUMERIC chuỗi của Debezium; Silver chuẩn hóa theo schema nguồn. Job append-only, không áp update/delete thành trạng thái hiện tại ở Bronze.

Table properties: format V2, unpartitioned, Parquet, Zstd, target 134217728 bytes (128 MiB), distribution none và contract version 1. Target là mục tiêu file, không phải kích thước tối thiểu hoặc số file cố định. Native Iceberg sink dùng query ID/epoch ID để nhận diện batch đã commit khi retry cùng checkpoint; vẫn cần nghiệm thu recovery thực tế.

### 5.2. Chuẩn bị và kiểm tra không ghi dữ liệu

Chạy từng khối trong Bash Ubuntu. Khai báo helper trong terminal hiện tại:

```bash
cd /home/thienduc/cdtn-lakehouse

dc() {
  docker compose \
    --project-name cdtn-lakehouse \
    --env-file /home/thienduc/cdtn-lakehouse/.env \
    -f /home/thienduc/cdtn-lakehouse/infra/compose.foundation.yml \
    -f /home/thienduc/cdtn-lakehouse/infra/compose.cdc.yml \
    -f /home/thienduc/cdtn-lakehouse/infra/compose.lakehouse.yml \
    "$@"
}
```

```bash
dc --profile bronze config --quiet
```

```bash
python scripts/ingest_olist_bronze.py validate --env-file .env
python scripts/check_olist_bronze.py validate --env-file .env
```

Kỳ vọng: Compose mã thoát 0; hai script in cấu hình hợp lệ cho orders/order_items. `validate` không import PySpark, tạo directory, kết nối Kafka/catalog hoặc ghi dữ liệu. Không cần requirements mới; PySpark/JAR đã nằm trong image/config Spark. `.env` dùng các biến sau, environment của shell/container có ưu tiên cao hơn khi kiểm tra:

| Biến | Mặc định triển khai |
| --- | --- |
| `BRONZE_TABLES` | orders,order_items |
| `BRONZE_KAFKA_BOOTSTRAP_SERVERS` | kafka:9092, địa chỉ nội bộ Compose |
| `BRONZE_TOPIC_PREFIX` | olist_pg.olist. |
| `BRONZE_CHECKPOINT_ROOT` | /opt/spark/work-dir/olist-bronze-checkpoints |
| `BRONZE_TRIGGER_SECONDS` | 60 |
| `BRONZE_MAX_OFFSETS_PER_TRIGGER` | 100000 cho mỗi query, không phải tốc độ broker |

### 5.3. Chuẩn bị dịch vụ và áp DDL một lần

Đảm bảo CDC đang hoạt động theo mục 2.1. Sau khi sửa Compose lần đầu, làm mới runner để nhận mounts và biến mới:

```bash
dc up -d postgres-catalog minio minio-init spark trino
```

Service `spark-bronze` có profile opt-in nên lệnh này chưa ingest. Chỉ sau khi runner sẵn sàng mới áp DDL bằng Spark, không dùng psql:

```bash
dc exec -T spark /opt/spark/bin/spark-sql \
  -f /opt/olist/sql/004_olist_bronze.sql
```

Kỳ vọng: namespace bronze và 7 bảng sự kiện được tạo. `IF NOT EXISTS` giúp chạy lại mà không xóa dữ liệu, nhưng không nâng cấp schema cũ. Bảng `lakehouse_smoke` nếu còn tồn tại là bảng thử riêng, không phải một trong 7 bảng sự kiện.

Kiểm tra khoảng offset Kafka trước khi bật job:

```bash
dc exec -T spark /opt/spark/bin/spark-submit \
  /opt/olist/scripts/check_olist_bronze.py bounds
```

Kỳ vọng mỗi bảng được chọn có partition bounds gồm `start_inclusive`, `end_exclusive`. Lần ingest đầu cần earliest còn 0. Nếu Kafka đã hết lịch sử ban đầu, dừng thiết lập để lập kế hoạch backfill tại mốc nhất quán; không tự xóa slot/topic hoặc đổi startingOffsets sang latest. Offset có thể có khoảng trống, không lấy hiệu end-start làm số message.

### 5.4. Khởi chạy và theo dõi liên tục

**Lệnh tiếp theo bắt đầu ghi Bronze.** Nó không sửa 8 bảng PostgreSQL nguồn:

```bash
dc up -d spark-bronze
```

Đích danh `spark-bronze` kích hoạt service dù profile chưa được bật toàn cục. Job dùng Spark local[2], một query cho orders và một cho order_items. Checkpoint mỗi query ở `<BRONZE_CHECKPOINT_ROOT>/<table>`, trên volume `cdtn-lakehouse_bronze_checkpoints` mount vào `/opt/spark/work-dir`. User spark có UID/GID 185 theo image hiện tại; mount work-dir sẵn có giữ quyền khởi tạo volume phù hợp.

```bash
dc logs --tail=100 -f spark-bronze
```

Kỳ vọng log `Started olist_bronze_orders` và `Started olist_bronze_order_items`; lần đầu có thể cần tải JAR. Ctrl+C ở lệnh logs chỉ dừng xem log, không dừng job. Sau khi đọc hết phần hiện có, query tiếp tục chờ và ghi sự kiện mới theo trigger, không cần bật lại mỗi lần PostgreSQL commit.

```bash
dc ps -a spark-bronze
```

Kỳ vọng service đang chạy, health chuyển healthy sau startup. Health đọc heartbeat không quá 90 giây và các query còn active; không chứng minh dữ liệu đã bắt kịp hoặc không có parse_error. Xem progress/offset/batch ID trong status JSON:

```bash
dc exec -T spark-bronze python3 -c \
  'from ingest_olist_bronze import read_settings; print(read_settings().status_file.read_text())' \
| jq .
```

Marker `olist-identity.json` ghi topic/bootstrap/target/table UUID. Khóa `<table>.lock` chống hai process cùng dùng checkpoint volume. Job từ chối bảng non-empty mất checkpoint/offset log, UUID/topic thay đổi hoặc checkpoint không nhận diện; không tự xóa hay ghi đè checkpoint để sửa lỗi. `ingest_olist_bronze.py run --available-now` có hỗ trợ drain rồi thoát, nhưng không chạy song song với service đang giữ checkpoint; quy trình mặc định ở đây dùng service liên tục.

### 5.5. Xem kết quả qua Trino và MinIO

```bash
dc exec -T trino trino --user bronze-inspect --execute \
  'SHOW TABLES FROM lakehouse.bronze'
```

Kỳ vọng có 7 bảng sự kiện, nhưng chỉ hai bảng được chọn đang được ingest. Xem số sự kiện theo loại:

```bash
dc exec -T trino trino --user bronze-inspect --execute '
SELECT op, is_tombstone, count(*) AS events
FROM lakehouse.bronze.orders
GROUP BY op, is_tombstone
ORDER BY op;
'
```

Khi snapshot gốc còn đủ, số op=r kỳ vọng: orders 99441; order_items 112650. Tổng Bronze có thể lớn hơn vì còn c/u/d/tombstone; không so tổng này trực tiếp với COUNT(*) PostgreSQL hiện tại. Không dùng hai con số baseline làm điều kiện duy nhất để công nhận đúng từng trường.

```bash
dc exec -T trino trino --user bronze-inspect --execute '
SELECT kafka_partition, kafka_offset, key_json, op,
       after_json, is_tombstone, parse_error
FROM lakehouse.bronze.orders
ORDER BY kafka_partition, kafka_offset DESC
LIMIT 10;
'
```

```bash
dc exec -T trino trino --user bronze-inspect --execute '
SELECT file_path, record_count, file_size_in_bytes
FROM lakehouse.bronze."orders$files";
'
```

Giữ dấu nháy đơn của toàn bộ SQL để Bash không mở rộng `$files`. Trong MinIO Console `http://localhost:9001`, mở bucket `olist-lakehouse` → warehouse/bronze/orders và order_items. Metadata/data file xuất hiện sau commit; kích thước có thể nhỏ hơn target 128 MiB. Tên user CLI không tự cấp quyền chỉ đọc; phân quyền Trino vẫn là việc triển khai riêng.

### 5.6. Đối soát chỉ đọc theo snapshot và khoảng offset

Xem trạng thái đã commit:

```bash
dc exec -T spark /opt/spark/bin/spark-submit \
  /opt/olist/scripts/check_olist_bronze.py status
```

Kỳ vọng các dòng JSON có snapshot ID, số rows, min/max offset, số parse_error/tombstone/op. `status` là kiểm kê; mã thoát 0 không thay thế `verify`.

```bash
dc exec -T spark /opt/spark/bin/spark-submit \
  /opt/olist/scripts/check_olist_bronze.py verify
```

Checker capture offset range `[earliest, latest)` từng partition và snapshot Iceberg cho từng bảng; read batch Kafka được giới hạn bằng các offset cụ thể, không dùng consumer commit. So identity thật, raw_key/raw_value đúng byte, timestamp/type và toàn bộ cột parse. `ingested_at` chỉ kiểm tra không NULL vì thời điểm ingest khác thời gian Kafka.

Điều kiện đạt trong phạm vi capture: `passed=true`; missing/extra/duplicate_groups/mismatched_rows/parse_errors/invalid metadata đều 0. `mismatched_columns` rỗng. Các bản ghi nằm ngoài khoảng capture được báo `bronze_rows_outside_range`, không âm thầm gọi là đã đối soát. Mã thoát 0 là đạt; 1 là không đạt điều kiện đối soát; lỗi cấu hình/nguồn đọc có mã khác 0. Không in raw dữ liệu khi báo lỗi, chỉ in tối đa 10 identity mẫu.

Nếu missing > 0 khi stream còn đang bắt kịp, xem source endOffset/progress, chờ commit rồi chạy lại. Các snapshot/ranges của hai bảng không phải giao dịch liên bảng. Checker này không thay thế đối soát PostgreSQL/Silver và không chứng minh mọi lịch sử đã từng có trong Kafka còn tồn tại.

Nếu earliest > 0, mặc định `verify` từ chối kết luận về lịch sử đầu. Với pipeline đã chạy lâu, có thể chọn kiểm tra riêng phần Kafka còn lưu:

```bash
dc exec -T spark /opt/spark/bin/spark-submit \
  /opt/olist/scripts/check_olist_bronze.py verify --allow-retained-only
```

Phải ghi phạm vi retained-only khi lưu kết quả; không gọi đây là đối soát toàn bộ snapshot nguồn. Nếu retention/truncation thay đổi range trong lúc kiểm tra, checker yêu cầu chạy lại ở mốc ổn định. Record có parse_error vẫn nằm ở Bronze, nhưng nghiệm thu không đạt cho đến khi có phương án xử lý đúng; không xóa checkpoint để nạp chồng.

### 5.7. Mở rộng đủ 7 bảng

Sau khi hai bảng đầu đạt kiểm tra, sửa duy nhất giá trị sau trong `.env`:

```dotenv
BRONZE_TABLES=all
```

```bash
dc up -d --force-recreate spark-bronze
```

Compose recreate để đọc env mới; `restart` đơn thuần không nạp lại env. Volume checkpoint giữ nguyên: hai query cũ tiếp tục từ checkpoint, năm query mới bắt đầu earliest nếu Kafka còn lịch sử và bảng đích rỗng. Không xóa checkpoint orders/order_items khi mở rộng.

Runner `spark` có thể vẫn giữ env chọn hai bảng cũ; dùng CLI override để kiểm tra đủ 7 mà không cần recreate runner:

```bash
dc exec -T spark /opt/spark/bin/spark-submit \
  /opt/olist/scripts/check_olist_bronze.py verify --tables all
```

Kỳ vọng 7 báo cáo passed=true. Bảy tổng op=r của snapshot gốc lần lượt là customers 99441, sellers 3095, products 32951, orders 99441, order_items 112650, order_payments 103886, order_reviews 99224; tổng 550688 khi lịch sử ban đầu còn nguyên. C/u/d/tombstone làm tổng sự kiện lớn hơn số dòng nguồn.

### 5.8. Dừng, khởi động lại và bằng chứng phục hồi

```bash
dc stop spark-bronze
```

Lệnh dừng có thời gian chờ 120 giây; named volume vẫn được giữ. Khi mở máy lại, khởi động foundation/CDC theo mục 2 và 4, sau đó:

```bash
dc up -d spark-bronze
```

Kỳ vọng log dùng cùng checkpoint, query ID ổn định với mỗi query còn run ID thay đổi; native Iceberg nhận diện epoch đã commit. Lưu bounds/status/verify trước và sau restart cùng snapshot ID/offset, rồi xác nhận sự kiện nguồn mới vẫn đến. Kịch bản CRUD có khôi phục nguồn được hướng dẫn riêng trong phiên chat; không thao tác các dòng Olist thật chỉ để thử restart.

Nếu dừng toàn pipeline, dừng `spark-bronze` trước Kafka/MinIO/catalog. Không xóa volume `bronze_checkpoints`, sửa metadata/offset log bằng tay, DROP/REPLACE bảng Bronze đang chạy hoặc expire/rollback snapshot streaming tùy tiện. Spark checkpoint và snapshot Iceberg giữ hai loại trạng thái riêng; cần backup/phục hồi có kế hoạch.

### 5.9. Xử lý lỗi và phần chưa nghiệm thu

| Dấu hiệu | Hành động |
| --- | --- |
| Missing target/schema/contract khác | Kiểm tra DDL 004 và catalog đúng tên; IF NOT EXISTS không sửa schema đã có. Không DROP bảng có dữ liệu để chữa nhanh. |
| Permission denied ở checkpoint | Kiểm tra mount work-dir và UID/GID của image/volume; cấu hình hiện chọn directory image sở hữu. Không đổi sang /tmp vì mất trạng thái khi recreate. |
| Table non-empty, checkpoint mất | Dừng; tìm lại volume/checkpoint đúng. Muốn rebuild phải có kế hoạch mốc dữ liệu, bảng và checkpoint mới được duyệt. |
| Kafka earliest > 0 ở lần đầu | Lập kế hoạch backfill từ nguồn nhất quán; không dùng latest để bỏ qua dữ liệu cần nạp. |
| parse_error hoặc mismatched_columns | Giữ raw bytes, xem identity mẫu và kiểm tra envelope/parser; không lọc bỏ message lỗi rồi công nhận đạt. |
| Healthy nhưng missing > 0 | Kiểm tra lag/progress và commit gần nhất, chạy lại audit sau khi bắt kịp. |
| Profile bronze chưa chạy | `up spark-bronze` mới khởi động ingest; runner spark healthy không thay thế service này. |

Trong lần viết code mới chỉ kiểm tra cấu hình và logic bằng dữ liệu tổng hợp. Startup/native commit/restart, byte fidelity trên Kafka thật và Trino đọc đủ 7 bảng vẫn cần chạy theo quy trình trên trước khi công nhận hoàn tất chặng Bronze.

Nguồn: [Iceberg streaming writes](https://iceberg.apache.org/docs/latest/spark-structured-streaming/), [Spark Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html), [native query/epoch commit trong Iceberg 1.12.0](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java).

## 6. Chặng sau Bronze

Chỉ công nhận Kafka → Bronze sau khi đối soát đủ phạm vi và lưu bằng chứng phục hồi. Tiếp theo kiểm chứng dbt-spark/SQL endpoint, tạo Silver trạng thái hiện tại và bảng dịch danh mục qua Spark JDBC, rồi Gold/KPI và Superset qua Trino. Silver chọn sự kiện cuối theo khóa trước khi lọc delete; dbt merge không tự áp dụng op=d. Quy trình POC/model mới được hướng dẫn riêng trước khi trở thành lệnh vận hành chính thức.
