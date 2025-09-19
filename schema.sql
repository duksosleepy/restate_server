CREATE TABLE orders (
    order_id VARCHAR NOT NULL, -- Ma_Don_Hang
    customer_name VARCHAR, -- Ten_Khach_Hang
    phone_number VARCHAR, -- So_Dien_Thoai
    document_type VARCHAR, -- Ma_Ct
    document_number VARCHAR, -- So_Ct
    department_code VARCHAR, -- Ma_BP
    order_date DATE, -- Ngay_Ct
    province VARCHAR, -- Tinh_Thanh
    district VARCHAR, -- Quan_Huyen
    ward VARCHAR, -- Phuong_Xa
    address TEXT, -- Dia_Chi
    product_code VARCHAR, -- Ma_Hang_Old
    product_name VARCHAR, -- Ten_Hang
    imei VARCHAR,
    quantity INTEGER, -- So_Luong
    revenue REAL, -- Doanh_Thu
    source_type VARCHAR CHECK (source_type IN ('online', 'offline')),
    status VARCHAR DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'needs_retry', 'completed')),
    error_code VARCHAR, -- API errorCode response
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    -- PRIMARY KEY(order_id) -- Uncomment if order_id is unique
);

CREATE TABLE non_existing_codes (
    product_code VARCHAR PRIMARY KEY,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_id VARCHAR
);

CREATE TABLE daily_task_stats (
    stat_date DATE PRIMARY KEY,
    completed_tasks INTEGER DEFAULT 0, -- Successfully processed orders
    failed_tasks INTEGER DEFAULT 0,    -- Orders that need retry
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_orders_order_id ON orders(order_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_orders_identifiers ON orders(order_id, product_code, imei);
CREATE INDEX idx_non_existing_codes_product_code ON non_existing_codes(product_code);
CREATE INDEX idx_daily_task_stats_date ON daily_task_stats(stat_date);
