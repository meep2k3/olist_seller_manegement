"""
Khởi tạo cấu trúc cơ sở dữ liệu hoàn chỉnh cho Olist (Brazilian E-commerce)
- Schema: raw_data (bảng dữ liệu thô để import)
- Schema: staging (dữ liệu đã được làm sạch)
- Schema: warehouse (dimension/fact/aggregate tables)
"""

import psycopg2
from psycopg2 import sql

# Cấu hình cơ sở dữ liệuimpo
DB_CONFIG = {
    "dbname": "olist_db",
    "user": "postgres",
    "password": "YOUR_DB_PASSWORD",
    "host": "localhost",
    "port": 5432,
}

# TẠO SCHEMA

CREATE_SCHEMAS = """
-- Tạo schema
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
"""

# TẠO BẢNG DỮ LIỆU THÔ (schema: raw_data)

RAW_TABLES = [
    # 1. Geolocation
    """
    CREATE TABLE IF NOT EXISTS raw_data.geolocation (
        id SERIAL PRIMARY KEY,
        geolocation_zip_code_prefix VARCHAR(10) NOT NULL,
        geolocation_lat DOUBLE PRECISION NOT NULL,
        geolocation_lng DOUBLE PRECISION NOT NULL,
        geolocation_city TEXT,
        geolocation_state VARCHAR(10)
    );
    """,

    # 2. Customers
    """
    CREATE TABLE IF NOT EXISTS raw_data.customers (
        customer_id VARCHAR(100) PRIMARY KEY,
        customer_unique_id VARCHAR(100) NOT NULL,
        customer_zip_code_prefix VARCHAR(10),
        customer_city TEXT,
        customer_state VARCHAR(10)
    );
    """,

    # 3. Sellers
    """
    CREATE TABLE IF NOT EXISTS raw_data.sellers (
        seller_id VARCHAR(100) PRIMARY KEY,
        seller_zip_code_prefix VARCHAR(10),
        seller_city TEXT,
        seller_state VARCHAR(10)
    );
    """,

    # 4. Products
    """
    CREATE TABLE IF NOT EXISTS raw_data.products (
        product_id VARCHAR(100) PRIMARY KEY,
        product_category_name TEXT,
        product_name_lenght DOUBLE PRECISION,
        product_description_lenght DOUBLE PRECISION,
        product_photos_qty DOUBLE PRECISION,
        product_weight_g DOUBLE PRECISION,
        product_length_cm DOUBLE PRECISION,
        product_height_cm DOUBLE PRECISION,
        product_width_cm DOUBLE PRECISION
    );
    """,

    # 5. Product Category Translation
    """
    CREATE TABLE IF NOT EXISTS raw_data.product_category_name_translation (
        product_category_name TEXT PRIMARY KEY,
        product_category_name_english TEXT
    );
    """,

    # 6. Orders
    """
    CREATE TABLE IF NOT EXISTS raw_data.orders (
        order_id VARCHAR(100) PRIMARY KEY,
        customer_id VARCHAR(100),
        order_status VARCHAR(50),
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP
    );
    """,

    # 7. Order Items
    """
    CREATE TABLE IF NOT EXISTS raw_data.order_items (
        order_id VARCHAR(100),
        order_item_id INTEGER,
        product_id VARCHAR(100),
        seller_id VARCHAR(100),
        shipping_limit_date TIMESTAMP,
        price DOUBLE PRECISION,
        freight_value DOUBLE PRECISION,
        PRIMARY KEY (order_id, order_item_id)
    );
    """,

    # 8. Order Payments
    """
    CREATE TABLE IF NOT EXISTS raw_data.payments (
        order_id VARCHAR(100),
        payment_sequential INTEGER,
        payment_type VARCHAR(50),
        payment_installments INTEGER,
        payment_value DOUBLE PRECISION,
        PRIMARY KEY (order_id, payment_sequential)
    );
    """,

    # 9. Order Reviews
    """
    CREATE TABLE IF NOT EXISTS raw_data.reviews (
        review_id VARCHAR(100),
        order_id VARCHAR(100),
        review_score INTEGER,
        review_comment_title TEXT,
        review_comment_message TEXT,
        review_creation_date TIMESTAMP,
        review_answer_timestamp TIMESTAMP
    );
    """
]

# HÀM THỰC THI

def create_schemas(conn):
    """Tạo schema"""
    print("\n" + "="*60)
    print("ĐANG TẠO SCHEMA")
    print("="*60)
    with conn.cursor() as cur:
        cur.execute(CREATE_SCHEMAS)
        conn.commit()
    print("Đã tạo các schema: raw_data, staging, warehouse")

def create_raw_tables(conn):
    """Tạo các bảng dữ liệu thô"""
    print("\n" + "="*60)
    print("ĐANG TẠO CÁC BẢNG DỮ LIỆU THÔ (schema: raw_data)")
    print("="*60)
    with conn.cursor() as cur:
        for i, statement in enumerate(RAW_TABLES, 1):
            cur.execute(statement)
            print(f" Đã tạo bảng {i}/{len(RAW_TABLES)}")
        conn.commit()
    print(" Hoàn thành tạo tất cả bảng raw")

def verify_setup(conn):
    """Xác minh thiết lập cơ sở dữ liệu"""
    print("\n" + "="*60)
    print("XÁC MINH THIẾT LẬP")
    print("="*60)
    
    with conn.cursor() as cur:
        # Kiểm tra các schema
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('raw_data', 'staging', 'warehouse')
            ORDER BY schema_name
        """)
        schemas = cur.fetchall()
        print(f"\n Tìm thấy các schema: {[s[0] for s in schemas]}")
        
        # Đếm số bảng
        for schema in ['raw_data']:
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}'
            """)
            count = cur.fetchone()[0]
            print(f" {schema}: {count} bảng")

def main():
    """Thực thi chính"""
    conn = None
    try:
        print("KHỞI TẠO CƠ SỞ DỮ LIỆU OLIST")
        print(f"\nĐang kết nối tới cơ sở dữ liệu: {DB_CONFIG['dbname']}")
        print(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        
        conn = psycopg2.connect(**DB_CONFIG)
        print(" Kết nối thành công")
        
        # Tạo schema
        create_schemas(conn)
        
        # Tạo các bảng raw
        create_raw_tables(conn)
        
        # Kiểm tra xác minh
        verify_setup(conn)
        
        print("HOÀN TẤT KHỞI TẠO CƠ SỞ DỮ LIỆU!")
        
    except Exception as e:
        print(f"\n Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print(" Đã đóng kết nối cơ sở dữ liệu\n")

if __name__ == "__main__":
    main()

