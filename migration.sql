-- Migration script to add 30-day retry window support and performance optimizations
-- Run this if you already have an existing database

-- Add first_failure_time column if it doesn't exist (SQLite doesn't have ALTER IF NOT EXISTS)
-- This column tracks when an order first failed for the 30-day retry window
-- You can check if column exists first:
-- SELECT COUNT(*) FROM pragma_table_info('orders') WHERE name='first_failure_time';

-- If the column doesn't exist, run:
-- ALTER TABLE orders ADD COLUMN first_failure_time TEXT;

-- For safety, here's a script that checks and adds the column:
-- (You'll need to run this in a SQLite client that supports conditionals)

-- Performance indexes (safe to run multiple times - IF NOT EXISTS)
CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders(order_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_status_updated ON orders(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_first_failure ON orders(first_failure_time);
CREATE INDEX IF NOT EXISTS idx_orders_product_code ON orders(product_code);
CREATE INDEX IF NOT EXISTS idx_non_existing_codes_product ON non_existing_codes(product_code);

-- Update existing records to set first_failure_time based on updated_at if NULL
UPDATE orders
SET first_failure_time = updated_at
WHERE first_failure_time IS NULL AND status = 'needs_retry';

-- Verify indexes were created
SELECT name, tbl_name, sql FROM sqlite_master
WHERE type = 'index' AND tbl_name IN ('orders', 'non_existing_codes')
ORDER BY tbl_name, name;
