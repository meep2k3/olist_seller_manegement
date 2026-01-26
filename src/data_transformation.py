"""
Biến đổi dữ liệu (Tối ưu hóa ELT)
- Sử dụng 'CREATE TABLE AS SELECT' để thực hiện biến đổi ngay trong Database.
- Đã loại bỏ các bảng phân tích thừa.
- Đã thêm ép kiểu tường minh (Type Casting) để đảm bảo Schema chuẩn.
- CẬP NHẬT: Đọc toàn bộ dữ liệu từ Staging (bao gồm bảng translation).
"""

import pandas as pd
import os
from sqlalchemy import text
import config
from config import get_db_engine, SCHEMA_WAREHOUSE
from data_loading import upload_to_gcs, load_gcs_to_bigquery, create_bq_dataset

def execute_elt_query(table_name, sql_query):
    """
    Hàm helper để chạy lệnh tạo bảng trong DB.
    Tự động Drop bảng cũ và Create bảng mới.
    """
    print(f"Đang tạo bảng {table_name}...")
    engine = get_db_engine()
    
    full_table_name = f"{SCHEMA_WAREHOUSE}.{table_name}"
    
    # Bọc query trong lệnh CREATE TABLE AS
    create_sql = f"""
    DROP TABLE IF EXISTS {full_table_name} CASCADE;
    CREATE TABLE {full_table_name} AS (
        {sql_query}
    );
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_sql))
            
        # Đếm số dòng để báo cáo
        count = pd.read_sql(f"SELECT COUNT(1) FROM {full_table_name}", engine).iloc[0,0]
        print(f"    Hoàn tất. Bảng {table_name} có {count:,} dòng.")
        return count
    except Exception as e:
        print(f"    LỖI khi tạo {table_name}: {e}")
        return 0

def create_warehouse_schema():
    """Tạo schema warehouse nếu chưa tồn tại"""
    engine = get_db_engine()
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_WAREHOUSE}"))
        conn.commit()
    print(f" Schema {SCHEMA_WAREHOUSE} đã sẵn sàng")

def create_dim_date():
    """
    Tạo bảng dim_date.
    Ép kiểu: Date -> DATE, Các phần tử tách ra -> INTEGER
    """
    sql = """
    WITH date_range AS (
        SELECT 
            MIN(order_purchase_timestamp::date) AS min_date,
            MAX(COALESCE(order_delivered_customer_date::date, order_estimated_delivery_date::date)) AS max_date
        FROM staging.orders_cleaned
    )
    SELECT 
        datum::date as date,
        EXTRACT(YEAR FROM datum)::INTEGER as year,
        EXTRACT(QUARTER FROM datum)::INTEGER as quarter,
        EXTRACT(MONTH FROM datum)::INTEGER as month,
        EXTRACT(WEEK FROM datum)::INTEGER as week,
        EXTRACT(DAY FROM datum)::INTEGER as day,
        EXTRACT(ISODOW FROM datum)::INTEGER as day_of_week,
        TO_CHAR(datum, 'Day')::VARCHAR(20) as day_name,
        TO_CHAR(datum, 'Month')::VARCHAR(20) as month_name,
        CASE 
            WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 1 
            ELSE 0 
        END::INTEGER as is_weekend
    FROM date_range, 
         generate_series(min_date, max_date, '1 day'::interval) as datum
    """
    return execute_elt_query('dim_date', sql)

def create_dim_customers():
    """
    Tạo bảng dim_customers.
    Ép kiểu chuẩn: VARCHAR cho ID và State.
    """
    sql = """
    SELECT 
        customer_id::VARCHAR(100),
        customer_unique_id::VARCHAR(100),
        customer_zip_code_prefix::VARCHAR(10),
        customer_city::TEXT,
        customer_state::VARCHAR(10)
    FROM staging.customers_cleaned
    """
    return execute_elt_query('dim_customers', sql)

def create_dim_products():
    """
    Tạo bảng dim_products.
    """
    sql = """
    SELECT 
        p.product_id::VARCHAR(100),
        p.product_category_name::TEXT,
        COALESCE(t.product_category_name_english, p.product_category_name)::TEXT AS category_english,
        p.product_name_lenght::DOUBLE PRECISION,
        p.product_description_lenght::DOUBLE PRECISION,
        p.product_photos_qty::DOUBLE PRECISION,
        p.product_weight_g::DOUBLE PRECISION,
        p.product_length_cm::DOUBLE PRECISION,
        p.product_height_cm::DOUBLE PRECISION,
        p.product_width_cm::DOUBLE PRECISION,
        (p.product_length_cm * p.product_height_cm * p.product_width_cm)::DOUBLE PRECISION AS product_volume_cm3
    FROM staging.products_cleaned p
    LEFT JOIN staging.product_category_name_translation t
        ON p.product_category_name = t.product_category_name
    """
    return execute_elt_query('dim_products', sql)

def create_dim_sellers():
    """
    Tạo bảng dim_sellers.
    """
    sql = """
    SELECT 
        seller_id::VARCHAR(100),
        seller_zip_code_prefix::VARCHAR(10),
        seller_city::TEXT,
        seller_state::VARCHAR(10)
    FROM staging.sellers_cleaned
    """
    return execute_elt_query('dim_sellers', sql)

def create_fact_order_items():
    """
    Tạo bảng Fact chi tiết (Item Level) trong Warehouse.
    Đây là bảng Bridge kết nối Order - Product - Seller.
    """
    sql = """
    SELECT 
        oi.order_id::VARCHAR(100),
        oi.order_item_id::INTEGER,
        oi.product_id::VARCHAR(100),
        oi.seller_id::VARCHAR(100),
        oi.shipping_limit_date::TIMESTAMP,
        oi.price::DOUBLE PRECISION,
        oi.freight_value::DOUBLE PRECISION
    FROM staging.order_items_cleaned oi
    """
    return execute_elt_query('fact_order_items', sql)

def create_fact_orders():
    """
    Tạo bảng Fact chính. 
    """
    sql = """
    WITH order_totals AS (
        SELECT 
            order_id,
            SUM(price) AS total_price,
            SUM(freight_value) AS total_freight,
            SUM(price + freight_value) AS total_amount,
            COUNT(*) AS item_count
        FROM staging.order_items_cleaned
        GROUP BY order_id
    ),
    order_reviews AS (
        SELECT 
            order_id,
            AVG(review_score) AS avg_review_score,
            COUNT(*) AS review_count
        FROM staging.reviews_cleaned
        GROUP BY order_id
    )
    SELECT 
        o.order_id::VARCHAR(100),
        o.customer_id::VARCHAR(100),
        o.order_status::VARCHAR(50),
        o.order_purchase_timestamp::TIMESTAMP,
        o.order_approved_at::TIMESTAMP,
        o.order_delivered_carrier_date::TIMESTAMP,
        o.order_delivered_customer_date::TIMESTAMP,
        o.order_estimated_delivery_date::TIMESTAMP,
        
        -- Tính toán và ép kiểu các cột dẫn xuất
        CASE 
            WHEN o.order_delivered_customer_date IS NOT NULL 
            THEN EXTRACT(DAY FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date))
            ELSE NULL 
        END::DOUBLE PRECISION AS delivery_delay_days,
        
        CASE 
            WHEN o.order_delivered_customer_date IS NOT NULL 
            THEN CASE WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date 
                 THEN 1 ELSE 0 END
            ELSE NULL 
        END::INTEGER AS on_time_delivery,
        
        CASE 
            WHEN o.order_delivered_customer_date IS NOT NULL 
            THEN EXTRACT(DAY FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))
            ELSE NULL 
        END::DOUBLE PRECISION AS actual_delivery_days,
        
        COALESCE(ot.total_price, 0)::DOUBLE PRECISION AS total_price,
        COALESCE(ot.total_freight, 0)::DOUBLE PRECISION AS total_freight,
        COALESCE(ot.total_amount, 0)::DOUBLE PRECISION AS total_amount,
        COALESCE(ot.item_count, 0)::INTEGER AS item_count,
        r.avg_review_score::DOUBLE PRECISION,
        COALESCE(r.review_count, 0)::INTEGER AS review_count
        
    FROM staging.orders_cleaned o
    LEFT JOIN order_totals ot ON o.order_id = ot.order_id
    LEFT JOIN order_reviews r ON o.order_id = r.order_id
    WHERE o.order_status IN ('delivered', 'shipped', 'invoiced')
    """
    return execute_elt_query('fact_orders', sql)

def sync_warehouse_to_cloud(engine):
    """
    Hàm này đọc các bảng Fact/Dim vừa tạo xong trong PostgreSQL 
    và đồng bộ chúng lên BigQuery.
    """
    print("\n Bắt đầu đồng bộ Data Warehouse lên Google Cloud...")
    
    # Danh sách các bảng trong Warehouse cần đẩy đi
    warehouse_tables = [
        "warehouse.fact_orders",
        "warehouse.fact_order_items",
        "warehouse.dim_sellers",
        "warehouse.dim_customers",
        "warehouse.dim_products"
    ]
    
    # Đảm bảo Dataset tồn tại
    dataset_name = "olist_analytics"
    create_bq_dataset(dataset_name)

    for table_full_name in warehouse_tables:
        try:
            table_clean_name = table_full_name.split('.')[-1] # Lấy tên dim_sellers
            csv_name = f"{table_clean_name}.csv"
            
            # 1. Extract: Đọc từ Postgres ra
            df = pd.read_sql(f"SELECT * FROM {table_full_name}", engine)
            df.to_csv(csv_name, index=False)
            
            # 2. Upload: Đẩy lên GCS
            gcs_path = f"warehouse/{csv_name}"
            upload_to_gcs(csv_name, gcs_path)
            
            # 3. Load: Đẩy vào BigQuery
            gcs_uri = f"gs://{config.GCS_BUCKET_NAME}/{gcs_path}"
            load_gcs_to_bigquery(gcs_uri, dataset_name, table_clean_name)
            
            # 4. Dọn dẹp file rác
            if os.path.exists(csv_name):
                os.remove(csv_name)
                
        except Exception as e:
            print(f"Không thể đồng bộ bảng {table_full_name}: {e}")


def run_transformation():
    """Chạy toàn bộ các bước biến đổi dữ liệu"""
    print("BIẾN ĐỔI DỮ LIỆU")

    create_warehouse_schema()
    
    stats = {}
    
    # Tạo bảng dimension
    print("\n Tạo bảng Dimension")
    stats['dim_date'] = create_dim_date()
    stats['dim_customers'] = create_dim_customers()
    stats['dim_products'] = create_dim_products()
    stats['dim_sellers'] = create_dim_sellers()
    
    # Tạo bảng fact
    print("\n Tạo bảng Fact")
    stats['fact_order_items'] = create_fact_order_items()
    stats['fact_orders'] = create_fact_orders()
    
    print("\n TỔNG KẾT BIẾN ĐỔI DỮ LIỆU:")
    for table, count in stats.items():
        print(f"  {table:30s}: {count:,} dòng")
    
    engine = get_db_engine()
    sync_warehouse_to_cloud(engine)

    print("\n Quá trình biến đổi & đồng bộ hoàn tất!\n")
    return stats

if __name__ == "__main__":
    run_transformation()
