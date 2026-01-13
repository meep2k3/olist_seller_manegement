"""
Tổng hợp dữ liệu (Tối ưu hóa ELT/ETL Hybrid)
- SQL thuần: Cho các tác vụ tổng hợp đơn giản (Nhanh hơn).
- Python: Cho các logic phức tạp (RFM, Sliding Window, Haversine).
- INTEGRATION: Sử dụng triệt để các bảng Fact/Dim từ Warehouse.
- CẬP NHẬT: Đọc Geolocation từ Staging thay vì Raw.
"""

import pandas as pd
import numpy as np
from sqlalchemy import text
from config import get_db_engine, SCHEMA_WAREHOUSE
from datetime import timedelta

def execute_sql_elt(task_name, sql_query):
    """Hàm chạy SQL thuần"""
    print(f"Đang tạo bảng {task_name}...")
    engine = get_db_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text(sql_query))
        
        # Đếm số dòng
        count = pd.read_sql(f"SELECT COUNT(1) FROM {SCHEMA_WAREHOUSE}.{task_name}", engine).iloc[0,0]
        print(f"   -> Hoàn tất. Bảng {task_name} có {count:,} dòng.")
        return count
    except Exception as e:
        print(f"   ERROR creating {task_name}: {e}")
        return 0

def create_agg_daily_sales():
    print("Đang tạo agg_daily_sales...")
    
    sql = """
    DROP TABLE IF EXISTS warehouse.agg_daily_sales;
    CREATE TABLE warehouse.agg_daily_sales AS (
        WITH daily_data AS (
            SELECT 
                fo.order_purchase_timestamp::date as date,
                SUM(fo.total_amount) as revenue,
                COUNT(DISTINCT fo.order_id) as orders,
                -- Đếm người dùng thực tế (Unique ID) thay vì customer_id đơn thuần
                COUNT(DISTINCT c.customer_unique_id) as customers
            FROM warehouse.fact_orders fo
            JOIN warehouse.dim_customers c ON fo.customer_id = c.customer_id
            WHERE fo.order_status IN ('delivered', 'shipped', 'invoiced')
            GROUP BY 1
        ),
        date_range AS (
            SELECT generate_series(MIN(date), MAX(date), '1 day'::interval)::date as d
            FROM daily_data
        )
        SELECT 
            dr.d as date,
            COALESCE(dd.revenue, 0) as revenue,
            COALESCE(dd.orders, 0) as orders,
            COALESCE(dd.customers, 0) as customers
        FROM date_range dr
        LEFT JOIN daily_data dd ON dr.d = dd.date
        ORDER BY dr.d
    );
    """
    return execute_sql_elt('agg_daily_sales', sql)

def create_agg_product_performance():
    sql = """
    DROP TABLE IF EXISTS warehouse.agg_product_performance;
    CREATE TABLE warehouse.agg_product_performance AS (
        SELECT 
            oi.product_id,
            SUM(oi.price) as revenue,
            COUNT(*) as quantity,
            COUNT(DISTINCT oi.order_id) as order_count,
            RANK() OVER (ORDER BY SUM(oi.price) DESC) as rank
        FROM staging.order_items_cleaned oi
        JOIN warehouse.fact_orders fo ON oi.order_id = fo.order_id
        WHERE fo.order_status IN ('delivered', 'shipped', 'invoiced')
        GROUP BY oi.product_id
    );
    """
    return execute_sql_elt('agg_product_performance', sql)

def create_agg_category_performance():
    sql = """
    DROP TABLE IF EXISTS warehouse.agg_category_performance;
    CREATE TABLE warehouse.agg_category_performance AS (
        SELECT 
            p.category_english as category,
            SUM(oi.price) as revenue,
            COUNT(DISTINCT oi.order_id) as orders,
            ROUND(AVG(oi.price)::numeric, 2) as avg_price
        FROM staging.order_items_cleaned oi
        JOIN warehouse.dim_products p ON oi.product_id = p.product_id
        JOIN warehouse.fact_orders fo ON oi.order_id = fo.order_id
        WHERE fo.order_status IN ('delivered', 'shipped', 'invoiced')
        GROUP BY p.category_english
        ORDER BY revenue DESC
    );
    """
    return execute_sql_elt('agg_category_performance', sql)

def create_agg_state_performance():
    sql = """
    DROP TABLE IF EXISTS warehouse.agg_state_performance;
    CREATE TABLE warehouse.agg_state_performance AS (
        SELECT 
            c.customer_state as state,
            SUM(fo.total_amount) as revenue,
            COUNT(DISTINCT fo.order_id) as orders,
            ROUND(AVG(fo.actual_delivery_days)::numeric, 2) as avg_delivery_days
        FROM warehouse.fact_orders fo
        JOIN warehouse.dim_customers c ON fo.customer_id = c.customer_id
        WHERE fo.order_status IN ('delivered', 'shipped', 'invoiced')
        GROUP BY c.customer_state
        ORDER BY revenue DESC
    );
    """
    return execute_sql_elt('agg_state_performance', sql)

def create_seller_evaluation():
    """
        Đang tạo bảng đầu vào cho seller_evaluation
    """
    sql_query = """
    DROP TABLE IF EXISTS warehouse.seller_evaluation;
    CREATE TABLE warehouse.seller_evaluation AS (
        SELECT 
            oi.seller_id,
            oi.order_id,
            oi.product_id,

            fo.order_status,
            fo.order_purchase_timestamp,
            fo.order_approved_at,
            fo.order_delivered_carrier_date,
            fo.order_delivered_customer_date,
            fo.order_estimated_delivery_date,
            oi.shipping_limit_date,

            r.review_score,
            r.review_comment_message,

            oi.price,
            oi.freight_value

        FROM warehouse.fact_order_items oi
        JOIN warehouse.fact_orders fo ON oi.order_id = fo.order_id
        LEFT JOIN staging.reviews_cleaned r ON fo.order_id = r.order_id

        WHERE fo.order_status IS NOT NULL
    );
    """
    return execute_sql_elt('seller_evaluation', sql_query)

def create_seller_segmentation():
    sql = """
    DROP TABLE IF EXISTS warehouse.seller_segmentation;
    CREATE TABLE warehouse.seller_segmentation AS (
        WITH order_metrics AS (
            -- Bước 1: Tổng hợp số liệu theo từng Đơn hàng (Order Level) trước
            SELECT 
                oi.seller_id,
                oi.order_id,
                SUM(oi.price) as order_value,
                SUM(oi.freight_value) as order_freight,
                MAX(la.distance_km) as distance_km, 
                MAX(c.customer_state) as customer_state
            FROM warehouse.fact_order_items oi
            JOIN warehouse.fact_orders fo ON oi.order_id = fo.order_id
            JOIN warehouse.dim_customers c ON fo.customer_id = c.customer_id
            LEFT JOIN warehouse.logistics_analytics la ON oi.order_id = la.order_id
            WHERE fo.order_status = 'delivered'
            GROUP BY oi.seller_id, oi.order_id
        ),
        
        item_metrics AS (
            -- Bước 2: Tổng hợp số liệu theo Sản phẩm (Item Level)
            SELECT 
                oi.seller_id,
                AVG(p.product_weight_g) as avg_item_weight_g,
                COUNT(DISTINCT p.product_category_name) as distinct_categories
            FROM warehouse.fact_order_items oi
            JOIN warehouse.dim_products p ON oi.product_id = p.product_id
            GROUP BY oi.seller_id
        )

        -- Bước 3: Tổng hợp cuối cùng theo Seller
        SELECT 
            om.seller_id,
            COALESCE(MAX(im.avg_item_weight_g), 0) as avg_weight_g,
            COALESCE(MAX(im.distinct_categories), 1) as category_diversity,
            COUNT(DISTINCT om.customer_state) as market_reach,
            AVG(om.distance_km) as avg_distance_km,
            AVG(om.order_value) as avg_order_value,
            AVG(om.order_freight / NULLIF(om.order_value, 0)) as avg_freight_ratio

        FROM order_metrics om
        LEFT JOIN item_metrics im ON om.seller_id = im.seller_id
        GROUP BY om.seller_id
    );
    """
    return execute_sql_elt('seller_segmentation', sql)

def create_nlp_bad_review():
    sql = """
    DROP TABLE IF EXISTS warehouse.nlp_bad_review;
    CREATE TABLE warehouse.nlp_bad_review AS (
        SELECT review_score, review_comment_message
        FROM staging.reviews_cleaned
        WHERE review_score IN (1,2) AND review_comment_message IS NOT NULL
    );
    """
    return execute_sql_elt('nlp_bad_review', sql)

def create_nlp_good_review():
    sql = """
    DROP TABLE IF EXISTS warehouse.nlp_good_review;
    CREATE TABLE warehouse.nlp_good_review AS (
        SELECT review_score, review_comment_message
        FROM staging.reviews_cleaned
        WHERE review_score IN (4,5) AND review_comment_message IS NOT NULL
    );
    """
    return execute_sql_elt('nlp_good_review', sql)

# MAIN 
def run_aggregation():
    print("\nTỔNG HỢP DỮ LIỆU")
    
    create_agg_daily_sales()
    create_agg_product_performance()
    create_agg_category_performance()
    create_agg_state_performance()
    create_seller_evaluation()
    create_seller_segmentation()
    create_nlp_bad_review()
    create_nlp_good_review()

    print("\nHoàn tất quy trình tổng hợp.")

    
if __name__ == "__main__":
    run_aggregation()
