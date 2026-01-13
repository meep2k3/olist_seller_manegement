"""
File cấu hình cơ sở dữ liệu cho ETL
Cập nhật: Đồng bộ với quy trình ELT/ETL Hybrid mới nhất
"""
import os
from sqlalchemy import create_engine

# Cấu hình kết nối PostgreSQL
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'olist_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'YOUR_DB_PASSWORD')
}

# Tạo Connection string
DB_CONNECTION_STRING = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

def get_db_engine():
    """Tạo và trả về đối tượng kết nối (engine) đến cơ sở dữ liệu"""
    return create_engine(DB_CONNECTION_STRING)

# Định nghĩa các schema
SCHEMA_RAW = 'raw_data'
SCHEMA_STAGING = 'staging'
SCHEMA_WAREHOUSE = 'warehouse'

# Định nghĩa tên các bảng
TABLES = {
    'raw': {
        'customers': f'{SCHEMA_RAW}.customers',
        'orders': f'{SCHEMA_RAW}.orders',
        'order_items': f'{SCHEMA_RAW}.order_items',
        'products': f'{SCHEMA_RAW}.products',
        'sellers': f'{SCHEMA_RAW}.sellers',
        'reviews': f'{SCHEMA_RAW}.reviews',
        'geolocation': f'{SCHEMA_RAW}.geolocation',
        'payments': f'{SCHEMA_RAW}.payments',
        'product_category_name_translation': f'{SCHEMA_RAW}.product_category_name_translation'
    },
    'staging': {
        'orders_cleaned': f'{SCHEMA_STAGING}.orders_cleaned',
        'order_items_cleaned': f'{SCHEMA_STAGING}.order_items_cleaned',
        'customers_cleaned': f'{SCHEMA_STAGING}.customers_cleaned',
        'products_cleaned': f'{SCHEMA_STAGING}.products_cleaned',
        'sellers_cleaned': f'{SCHEMA_STAGING}.sellers_cleaned',
        'reviews_cleaned': f'{SCHEMA_STAGING}.reviews_cleaned',
        'payments_cleaned': f'{SCHEMA_STAGING}.payments_cleaned',
        'geolocation': f'{SCHEMA_STAGING}.geolocation',
        'product_category_name_translation': f'{SCHEMA_STAGING}.product_category_name_translation'
    },
    'warehouse': {
        # Core Tables (Fact/Dim)
        'fact_orders': f'{SCHEMA_WAREHOUSE}.fact_orders',
        'dim_customers': f'{SCHEMA_WAREHOUSE}.dim_customers',
        'dim_products': f'{SCHEMA_WAREHOUSE}.dim_products',
        'dim_sellers': f'{SCHEMA_WAREHOUSE}.dim_sellers',
        'dim_date': f'{SCHEMA_WAREHOUSE}.dim_date',
        
        # Aggregate tables (Reporting)
        'agg_daily_sales': f'{SCHEMA_WAREHOUSE}.agg_daily_sales',
        'agg_product_performance': f'{SCHEMA_WAREHOUSE}.agg_product_performance',
        'agg_category_performance': f'{SCHEMA_WAREHOUSE}.agg_category_performance',
        'agg_state_performance': f'{SCHEMA_WAREHOUSE}.agg_state_performance',
        
        # Analysis tables (ML & Advanced Analytics)
        'customer_summary': f'{SCHEMA_WAREHOUSE}.customer_summary',
        'seller_scorecard': f'{SCHEMA_WAREHOUSE}.seller_scorecard',           
        'logistics_analytics': f'{SCHEMA_WAREHOUSE}.logistics_analytics',     
        'review_analysis_dataset': f'{SCHEMA_WAREHOUSE}.review_analysis_dataset',
        'product_associations': f'{SCHEMA_WAREHOUSE}.product_associations'
    }
}

# Quy tắc nghiệp vụ (Business Rules)
BUSINESS_RULES = {
    'min_price': 0,
    'max_price': 100000,
    'min_freight_value': 0,
    'max_freight_value': 10000,
    'valid_order_statuses': ['delivered', 'shipped', 'invoiced'], # Bỏ processing/canceled để tập trung vào doanh thu thực
    'min_delivery_days': 0,
    'max_delivery_days': 365
}
