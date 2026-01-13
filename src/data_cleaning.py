"""
Làm sạch dữ liệu (Staging Layer)
- Tích hợp logic xử lý kiểu dữ liệu (từ data_executed cũ)
- Loại bỏ dòng trùng lặp (Deduplication)
- Xử lý giá trị thiếu và quy tắc nghiệp vụ
"""

import pandas as pd
from config import get_db_engine, TABLES, BUSINESS_RULES, SCHEMA_STAGING

# CÁC HÀM HỖ TRỢ
def save_to_staging(df, table_name):
    """Lưu DataFrame vào Schema Staging với cấu hình tối ưu"""
    if df.empty:
        print(f"   Cảnh báo: Bảng {table_name} rỗng sau khi làm sạch!")
        return 0
        
    engine = get_db_engine()
    print(f"  -> Đang lưu {len(df):,} dòng vào staging.{table_name}...", end=' ')
    
    # Dùng method='multi' để tăng tốc insert
    df.to_sql(table_name, engine, schema=SCHEMA_STAGING, 
              if_exists='replace', index=False, 
              method='multi', chunksize=2000)
    print("Xong!")
    return len(df)

def copy_raw_to_staging(raw_table_key, staging_table_name):
    """Sao chép bảng đơn giản từ Raw -> Staging (Customers, Sellers, etc.)"""
    print(f"Sao chép {raw_table_key} -> staging.{staging_table_name}...")
    engine = get_db_engine()
    
    # Đọc từ Raw
    query = f"SELECT * FROM {TABLES['raw'][raw_table_key]}"
    df = pd.read_sql(query, engine)
    
    # Lưu sang Staging
    return save_to_staging(df, staging_table_name)

# CÁC HÀM XỬ LÝ CHÍNH
def clean_reviews():
    """
    Xử lý bảng Reviews:
    1. Ép kiểu datetime & numeric.
    2. DEDUPLICATION: Loại bỏ review_id trùng (giữ bản ghi mới nhất).
    3. Xử lý NULL.
    """
    print("Đang làm sạch bảng reviews...")
    engine = get_db_engine()
    
    # Đọc Raw Data
    query = f"SELECT * FROM {TABLES['raw']['reviews']}"
    df = pd.read_sql(query, engine)
    print(f"  Số dòng ban đầu: {len(df):,}")

    # Ép kiểu dữ liệu
    df['review_score'] = pd.to_numeric(df['review_score'], errors='coerce') 
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')

    # Xử lý trùng lặp
    # Sắp xếp để bản ghi mới nhất nằm cuối
    df = df.sort_values(
        by=['review_id', 'review_answer_timestamp', 'review_creation_date'],
        ascending=[True, True, True]
    )
    # Giữ lại dòng cuối cùng (mới nhất) cho mỗi review_id
    initial_count = len(df)
    df = df.drop_duplicates(subset=['review_id'], keep='last')
    
    if len(df) < initial_count:
        print(f"  -> Đã loại bỏ {initial_count - len(df):,} review trùng lặp.")

    # Điền giá trị thiếu cho text
    df['review_comment_title'] = df['review_comment_title'].fillna('')
    df['review_comment_message'] = df['review_comment_message'].fillna('')

    return save_to_staging(df, 'reviews_cleaned')


def clean_orders():
    """
    Xử lý bảng Orders:
    1. Ép kiểu 5 cột datetime.
    2. Lọc trạng thái đơn hàng.
    3. Kiểm tra logic thời gian (Ngày giao > Ngày mua...).
    """
    print("Đang làm sạch bảng orders...")
    engine = get_db_engine()
    
    query = f"SELECT * FROM {TABLES['raw']['orders']}"
    df = pd.read_sql(query, engine)
    print(f"  Số dòng ban đầu: {len(df):,}")

    # Ép kiểu datetime
    date_cols = [
        'order_purchase_timestamp', 'order_approved_at', 
        'order_delivered_carrier_date', 'order_delivered_customer_date', 
        'order_estimated_delivery_date'
    ]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Lọc trạng thái
    valid_statuses = BUSINESS_RULES['valid_order_statuses']
    df = df[df['order_status'].isin(valid_statuses)]
    
    # Loại bỏ đơn 'delivered' nhưng thiếu ngày giao
    invalid_delivered = (
        (df['order_status'] == 'delivered') & 
        (df['order_delivered_customer_date'].isna())
    )
    df = df[~invalid_delivered]
    
    # Logic thời gian: Ngày mua > Ngày giao
    invalid_dates = (
        (df['order_delivered_customer_date'].notna()) & 
        (df['order_purchase_timestamp'] > df['order_delivered_customer_date'])
    )
    df = df[~invalid_dates]
    
    # Logic thời gian: Ngày dự kiến < Ngày mua
    invalid_estimate = (
        (df['order_estimated_delivery_date'].notna()) & 
        (df['order_estimated_delivery_date'] < df['order_purchase_timestamp'])
    )
    df = df[~invalid_estimate]
    
    return save_to_staging(df, 'orders_cleaned')


def clean_order_items():
    """
    Xử lý bảng Order Items:
    1. Ép kiểu shipping_limit_date.
    2. Sao chép sang Staging.
    """
    print("Đang làm sạch bảng order_items...")
    engine = get_db_engine()
    
    query = f"SELECT * FROM {TABLES['raw']['order_items']}"
    df = pd.read_sql(query, engine)
    
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    
    # Đảm bảo numeric
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    df['freight_value'] = pd.to_numeric(df['freight_value'], errors='coerce').fillna(0)
    
    return save_to_staging(df, 'order_items_cleaned')


def clean_products():
    """
    Xử lý bảng Products:
    1. Ép kiểu số cho kích thước/trọng lượng.
    2. Điền giá trị thiếu (Median).
    3. Fix giá trị <= 0.
    """
    print("Đang làm sạch bảng products...")
    engine = get_db_engine()

    query = f"SELECT * FROM {TABLES['raw']['products']}"
    df = pd.read_sql(query, engine)

    # Xử lý tên danh mục
    if 'product_category_name' in df.columns:
        df['product_category_name'] = df['product_category_name'].fillna('unknown').astype(str)

    # Chuẩn hóa các cột số (Logic kết hợp)
    # Các cột text description -> fill 0
    text_numeric_cols = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
    for col in text_numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Các cột kích thước -> fill Median
    dim_cols = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
    for col in dim_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Tính median (bỏ qua NaN)
            median_val = df[col].median() if df[col].notna().any() else 0.0
            
            # Fill NaN
            df[col] = df[col].fillna(median_val)
            
            # Fix giá trị <= 0
            df.loc[df[col] <= 0, col] = median_val

    return save_to_staging(df, 'products_cleaned')


def run_cleaning():
    """Chạy toàn bộ quá trình làm sạch và đẩy vào Staging"""
    print("LÀM SẠCH & CHUẨN HÓA DỮ LIỆU (RAW -> STAGING)")

    stats = {}
    
    # Nhóm Copy trực tiếp (Các bảng 'tĩnh' hoặc ít lỗi)
    # Đảm bảo key khớp với config TABLES['raw']
    stats['customers'] = copy_raw_to_staging('customers', 'customers_cleaned')
    stats['sellers'] = copy_raw_to_staging('sellers', 'sellers_cleaned')
    stats['geolocation'] = copy_raw_to_staging('geolocation', 'geolocation') 
    stats['payments'] = copy_raw_to_staging('payments', 'payments_cleaned')
    stats['translation'] = copy_raw_to_staging('product_category_name_translation', 'product_category_name_translation')
    
    # Nhóm Xử lý Logic Phức tạp 
    stats['order_items'] = clean_order_items() 
    stats['orders'] = clean_orders()
    stats['products'] = clean_products()
    stats['reviews'] = clean_reviews()
    
    print("\nTỔNG KẾT GIAI ĐOẠN STAGING:")

    for table, count in stats.items():
        print(f"  {table:20s}: {count:,} dòng")
    print("\nQuá trình chuẩn bị dữ liệu Staging hoàn tất!\n")
    
    return stats

if __name__ == "__main__":
    run_cleaning()
