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
    revenue DECIMAL(15,2), -- Doanh_Thu
    source_type VARCHAR CHECK (source_type IN ('online', 'offline')),
    status VARCHAR DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'needs_retry', 'completed')),
    error_code VARCHAR, -- API errorCode response
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);


CREATE TABLE non_existing_codes (
    product_code VARCHAR PRIMARY KEY,
    detected_at TIMESTAMP DEFAULT NOW(),
    order_id VARCHAR
);

CREATE INDEX idx_orders_order_id ON orders(order_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);
-- Index for the new identification method (maDonHang, maHang, imei)
CREATE INDEX idx_orders_identifiers ON orders(order_id, product_code, imei);
-- Index for non-existing codes
CREATE INDEX idx_non_existing_codes_product_code ON non_existing_codes(product_code);
