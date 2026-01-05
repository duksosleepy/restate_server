-- Improved Restate Server Database Schema
-- TEXT for dates is correct for SQLite (ISO 8601 format recommended)
-- Added constraints for data integrity

-- Orders table with retry tracking and validation constraints
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    customer_name TEXT,
    phone_number TEXT,
    document_type TEXT,
    document_number TEXT,
    department_code TEXT,
    order_date TEXT,  -- ISO 8601 format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
    province TEXT,
    district TEXT,
    ward TEXT,
    address TEXT,
    product_code TEXT,
    product_name TEXT,
    imei TEXT,
    quantity REAL CHECK(quantity IS NULL OR quantity >= 0),  -- Non-negative if present
    revenue REAL CHECK(revenue IS NULL OR revenue >= 0),  -- Non-negative if present
    source_type TEXT CHECK(source_type IN ('online', 'offline')),  -- Constrain to valid values
    status TEXT NOT NULL DEFAULT 'needs_retry' CHECK(status IN ('needs_retry', 'completed', 'failed')),
    error_code TEXT,
    first_failure_time TEXT,  -- ISO 8601 format - tracks when order first failed
    updated_at TEXT NOT NULL,  -- ISO 8601 format
    UNIQUE(order_id, product_code, imei),
    -- Ensure date fields are valid ISO 8601 if present
    CHECK(order_date IS NULL OR order_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'),
    CHECK(first_failure_time IS NULL OR first_failure_time GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'),
    CHECK(updated_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*')
);

-- Daily task statistics with validation
CREATE TABLE IF NOT EXISTS daily_task_stats (
    stat_date TEXT PRIMARY KEY CHECK(stat_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),  -- ISO 8601 date
    completed_tasks INTEGER NOT NULL DEFAULT 0 CHECK(completed_tasks >= 0),
    failed_tasks INTEGER NOT NULL DEFAULT 0 CHECK(failed_tasks >= 0),
    last_updated TEXT NOT NULL CHECK(last_updated GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*')
);

-- Non-existing product codes tracking
CREATE TABLE IF NOT EXISTS non_existing_codes (
    product_code TEXT NOT NULL,
    order_id TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')) CHECK(created_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'),
    PRIMARY KEY (product_code, order_id)
);

-- Performance indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders(order_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_status_updated ON orders(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_first_failure ON orders(first_failure_time);
CREATE INDEX IF NOT EXISTS idx_orders_product_code ON orders(product_code);
CREATE INDEX IF NOT EXISTS idx_non_existing_codes_product ON non_existing_codes(product_code);

-- Enable WAL mode for better concurrency
PRAGMA journal_mode=WAL;

-- Optimize cache size (10MB cache)
PRAGMA cache_size=10000;

-- Use memory for temp storage
PRAGMA temp_store=MEMORY;
