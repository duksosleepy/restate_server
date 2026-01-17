-- Initialize Restate Server Database Schema
-- This schema supports 30-day retry window and optimized database performance

-- Orders table with retry tracking
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    customer_name TEXT,
    phone_number TEXT,
    document_type TEXT,
    document_number TEXT,
    department_code TEXT,
    order_date TEXT,
    province TEXT,
    district TEXT,
    ward TEXT,
    address TEXT,
    product_code TEXT,
    product_name TEXT,
    imei TEXT,
    quantity REAL,
    revenue REAL,
    source_type TEXT,
    status TEXT NOT NULL DEFAULT 'needs_retry',
    error_code TEXT,
    first_failure_time TEXT,  -- Track when the order first failed (for 30-day window)
    updated_at TEXT NOT NULL,
    UNIQUE(order_id, product_code, imei)
);

-- Daily task statistics
CREATE TABLE IF NOT EXISTS daily_task_stats (
    stat_date TEXT PRIMARY KEY,
    completed_tasks INTEGER DEFAULT 0,
    failed_tasks INTEGER DEFAULT 0,
    last_updated TEXT NOT NULL
);

-- Non-existing product codes tracking
CREATE TABLE IF NOT EXISTS non_existing_codes (
    product_code TEXT NOT NULL,
    order_id TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    email_sent INTEGER DEFAULT 0,  -- 0 = not sent, 1 = sent
    PRIMARY KEY (product_code, order_id)
);

-- Performance indexes for fast queries
-- Index on order_id for quick lookups
CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders(order_id);

-- Index on status for filtering retry-needed orders
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- Composite index for cleanup queries (status + updated_at)
CREATE INDEX IF NOT EXISTS idx_orders_status_updated ON orders(status, updated_at);

-- Index on first_failure_time for 30-day window checks
CREATE INDEX IF NOT EXISTS idx_orders_first_failure ON orders(first_failure_time);

-- Index on product_code for product-related queries
CREATE INDEX IF NOT EXISTS idx_orders_product_code ON orders(product_code);

-- Index on product_code in non_existing_codes for faster lookups
CREATE INDEX IF NOT EXISTS idx_non_existing_codes_product ON non_existing_codes(product_code);

-- Index on email_sent for querying unsent codes
CREATE INDEX IF NOT EXISTS idx_non_existing_codes_email_sent ON non_existing_codes(email_sent);

-- Enable WAL mode for better concurrency (recommended for SQLite)
PRAGMA journal_mode=WAL;

-- Optimize cache size (10MB cache)
PRAGMA cache_size=10000;

-- Use memory for temp storage
PRAGMA temp_store=MEMORY;
