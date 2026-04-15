import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, Optional

import httpx
import pandas as pd
import restate
from dotenv import load_dotenv
from pydantic import BaseModel
from restate import (
    Context,
    InvocationRetryPolicy,
    Service,
    TerminalError,
    app,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("server.log"), logging.StreamHandler()],
)

# Create a logger for database operations
db_logger = logging.getLogger("database_operations")

# Database configuration
DB_PATH = os.getenv("DB_PATH", "orders.db")
sqlite_lock = Lock()


class DatabaseManager:
    """Thread-safe SQLite3 database manager with connection pooling"""

    def __init__(self, db_path: str, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self._connections = []
        self._lock = Lock()
        self._initialize_database()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection from the pool or create new one"""
        with self._lock:
            if self._connections:
                return self._connections.pop()
            else:
                conn = sqlite3.connect(
                    self.db_path, timeout=30.0, check_same_thread=False
                )
                # Enable WAL mode for better concurrency
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=OFF")
                conn.execute("PRAGMA cache_size=10000")
                conn.execute("PRAGMA temp_store=MEMORY")
                return conn

    def _return_connection(self, conn: sqlite3.Connection):
        """Return a connection to the pool"""
        with self._lock:
            if len(self._connections) < self.max_connections:
                self._connections.append(conn)
            else:
                conn.close()

    def _initialize_database(self):
        """Initialize database tables if they don't exist with performance optimizations"""
        with self.execute_query() as conn:
            # Performance indexes for fast queries (safe to run - IF NOT EXISTS)
            # These indexes optimize the most common query patterns
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders(order_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_status_updated ON orders(status, updated_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_first_failure ON orders(first_failure_time)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_product_code ON orders(product_code)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_non_existing_codes_product ON non_existing_codes(product_code)"
            )

            conn.commit()
            db_logger.info(
                "Database initialization completed with performance indexes"
            )

    def execute_query(self):
        """Context manager for database operations"""
        return DatabaseConnection(self)


class DatabaseConnection:
    """Context manager for database connections"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.conn = None

    def __enter__(self) -> sqlite3.Connection:
        self.conn = self.db_manager._get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.db_manager._return_connection(self.conn)


# Initialize database manager
db_manager = DatabaseManager(DB_PATH)


# Data models
class HttpRequest(BaseModel):
    url: str
    data: Dict[str, Any]
    task_id: str


class HttpResponse(BaseModel):
    task_id: str
    url: str
    status_code: int
    response_data: Dict[str, Any]
    success: bool
    error: Optional[str] = None
    needs_manual_retry: bool = False


http_task = Service(
    "HttpTask",
    invocation_retry_policy=InvocationRetryPolicy(
        initial_interval=timedelta(minutes=15),
        exponentiation_factor=2.0,
        max_interval=timedelta(hours=24),  # Cap at 24 hours between retries
        max_attempts=100,  # Allow up to 100 retry attempts
        on_max_attempts='pause',  # Pause invocation instead of killing it after max attempts
    ),
)

# Thread-safe set for deduplication (used as cache, DB is source of truth)
non_existing_codes_cache: set = set()


def query_from_thread(query_func, *args):
    """Execute a database query in a thread-safe manner"""
    with sqlite_lock:
        try:
            result = query_func(*args)
            return result
        except Exception as e:
            db_logger.error(
                f"Database query failed for {query_func.__name__}: {str(e)}"
            )
            raise


def update_daily_stats_thread(
    task_completed: bool = False, task_failed: bool = False
):
    """Thread function to update daily task statistics"""
    today = datetime.now().date().isoformat()

    try:
        with db_manager.execute_query() as conn:
            # Insert or update daily stats
            if task_completed:
                conn.execute(
                    """
                    INSERT INTO daily_task_stats (stat_date, completed_tasks, failed_tasks, last_updated)
                    VALUES (?, 1, 0, ?)
                    ON CONFLICT(stat_date) DO UPDATE SET
                        completed_tasks = completed_tasks + 1,
                        last_updated = ?
                """,
                    (
                        today,
                        datetime.now().isoformat(),
                        datetime.now().isoformat(),
                    ),
                )

            if task_failed:
                conn.execute(
                    """
                    INSERT INTO daily_task_stats (stat_date, completed_tasks, failed_tasks, last_updated)
                    VALUES (?, 0, 1, ?)
                    ON CONFLICT(stat_date) DO UPDATE SET
                        failed_tasks = failed_tasks + 1,
                        last_updated = ?
                """,
                    (
                        today,
                        datetime.now().isoformat(),
                        datetime.now().isoformat(),
                    ),
                )
    except Exception as e:
        db_logger.error(f"Error updating daily stats: {str(e)}")
        raise


def insert_non_existing_code_thread(product_code: str, order_id: str):
    """Thread function to insert non-existing product code into database"""
    try:
        with db_manager.execute_query() as conn:
            # Use INSERT OR IGNORE to handle duplicates gracefully
            # email_sent defaults to 0 (not sent)
            conn.execute(
                """
                INSERT OR IGNORE INTO non_existing_codes (product_code, order_id, email_sent)
                VALUES (?, ?, 0)
            """,
                (product_code, order_id),
            )
    except Exception as e:
        db_logger.error(
            f"Error inserting non-existing code {product_code}: {str(e)}"
        )
        raise


def get_unsent_non_existing_codes_thread() -> list:
    """Get all non-existing codes that haven't been sent via email yet"""
    try:
        with db_manager.execute_query() as conn:
            cursor = conn.execute(
                """
                SELECT DISTINCT product_code FROM non_existing_codes
                WHERE email_sent = 0
                ORDER BY created_at ASC
            """
            )
            codes = [row[0] for row in cursor.fetchall()]
            return codes
    except Exception as e:
        db_logger.error(f"Error fetching unsent non-existing codes: {str(e)}")
        raise


def mark_codes_as_sent_thread(codes: list) -> int:
    """Mark the given product codes as sent in the database"""
    if not codes:
        return 0

    try:
        with db_manager.execute_query() as conn:
            # Use parameterized query with placeholders for each code
            placeholders = ",".join("?" * len(codes))
            conn.execute(
                f"""
                UPDATE non_existing_codes
                SET email_sent = 1
                WHERE product_code IN ({placeholders}) AND email_sent = 0
            """,
                codes,
            )
            updated_count = conn.total_changes
            return updated_count
    except Exception as e:
        db_logger.error(f"Error marking codes as sent: {str(e)}")
        raise


def insert_order_thread(request: HttpRequest, response: HttpResponse):
    """Thread function to insert new order in database
    Handles orders with multiple detail items (combined by maDonHang)"""
    db_logger.info(
        f"Starting insert_order_thread for task_id: {request.task_id}"
    )

    # Extract order data from the request
    try:
        order_data = request.data["data"][0]["master"]
        details_list = request.data["data"][0].get("detail", [])
    except (KeyError, IndexError):
        order_data = {}
        details_list = []

    # Determine source_type based on maDonHang
    ma_don_hang = order_data.get("maDonHang", "")
    source_type = "offline" if "/" in ma_don_hang else "online"

    if not details_list:
        db_logger.warning(f"No detail items found for task_id: {request.task_id}")
        return f"No details to insert for order {request.task_id}"

    try:
        with db_manager.execute_query() as conn:
            # Set first_failure_time to current time for new orders
            current_time = datetime.now().isoformat()

            # Insert each detail item as a separate order record
            inserted_count = 0
            for details in details_list:
                # Use INSERT OR REPLACE to handle duplicates
                conn.execute(
                    """
                    INSERT OR REPLACE INTO orders (
                        order_id, customer_name, phone_number, document_type, document_number,
                        department_code, order_date, province, district, ward, address,
                        product_code, product_name, imei, quantity, revenue, source_type,
                        status, error_code, first_failure_time, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        request.task_id,  # order_id
                        order_data.get("tenKhachHang"),  # customer_name
                        order_data.get("soDienThoai"),  # phone_number
                        order_data.get("maCT"),  # document_type
                        order_data.get("soCT"),  # document_number
                        order_data.get("maBoPhan"),  # department_code
                        order_data.get("ngayCT"),  # order_date
                        order_data.get("tinhThanh"),  # province
                        order_data.get("quanHuyen"),  # district
                        order_data.get("phuongXa"),  # ward
                        order_data.get("diaChi"),  # address
                        details.get("maHang"),  # product_code
                        details.get("tenHang"),  # product_name
                        details.get("imei"),  # imei
                        details.get("soLuong"),  # quantity
                        details.get("doanhThu"),  # revenue
                        source_type,  # source_type
                        "needs_retry",  # status
                        response.response_data.get(
                            "errorCode", response.error
                        ),  # error_code
                        current_time,  # first_failure_time
                        current_time,  # updated_at
                    ),
                )
                inserted_count += 1

        db_logger.info(
            f"Completed insert_order_thread for task_id: {request.task_id}, inserted {inserted_count} detail items"
        )
        return f"Inserted new order {request.task_id} with {inserted_count} detail items to database"
    except Exception as e:
        db_logger.error(f"Error inserting order {request.task_id}: {str(e)}")
        raise


def update_order_thread(request: HttpRequest, response: HttpResponse):
    """Thread function to update existing order based on order_id, product_code, and imei
    Handles orders with multiple detail items (combined by maDonHang)"""
    db_logger.info(
        f"Starting update_order_thread for task_id: {request.task_id}"
    )

    # Extract order data from the request
    try:
        order_data = request.data["data"][0]["master"]
        details_list = request.data["data"][0].get("detail", [])
    except (KeyError, IndexError):
        order_data = {}
        details_list = []

    # Determine source_type based on maDonHang
    ma_don_hang = order_data.get("maDonHang", "")
    source_type = "offline" if "/" in ma_don_hang else "online"

    if not details_list:
        db_logger.warning(f"No detail items found for task_id: {request.task_id}")
        return f"No details to update for order {request.task_id}"

    try:
        with db_manager.execute_query() as conn:
            # Update each detail item - update existing records but preserve first_failure_time
            # Only update updated_at, not first_failure_time
            updated_count = 0
            for details in details_list:
                conn.execute(
                    """
                    UPDATE orders
                    SET
                        customer_name = ?,
                        phone_number = ?,
                        document_type = ?,
                        document_number = ?,
                        department_code = ?,
                        order_date = ?,
                        province = ?,
                        district = ?,
                        ward = ?,
                        address = ?,
                        product_code = ?,
                        product_name = ?,
                        imei = ?,
                        quantity = ?,
                        revenue = ?,
                        source_type = ?,
                        error_code = ?,
                        updated_at = ?,
                        status = 'needs_retry'
                    WHERE order_id = ? AND product_code = ? AND imei = ?
                """,
                    (
                        order_data.get("tenKhachHang"),  # customer_name
                        order_data.get("soDienThoai"),  # phone_number
                        order_data.get("maCT"),  # document_type
                        order_data.get("soCT"),  # document_number
                        order_data.get("maBoPhan"),  # department_code
                        order_data.get("ngayCT"),  # order_date
                        order_data.get("tinhThanh"),  # province
                        order_data.get("quanHuyen"),  # district
                        order_data.get("phuongXa"),  # ward
                        order_data.get("diaChi"),  # address
                        details.get("maHang"),  # product_code
                        details.get("tenHang"),  # product_name
                        details.get("imei"),  # imei
                        details.get("soLuong"),  # quantity
                        details.get("doanhThu"),  # revenue
                        source_type,  # source_type
                        response.response_data.get(
                            "errorCode", response.error
                        ),  # error_code
                        datetime.now().isoformat(),  # updated_at
                        request.task_id,  # WHERE order_id
                        details.get("maHang"),  # WHERE product_code
                        details.get("imei"),  # WHERE imei
                    ),
                )
                updated_count += 1

        db_logger.info(
            f"Completed update_order_thread for task_id: {request.task_id}, updated {updated_count} detail items"
        )
        return f"Updated existing order {request.task_id} with {updated_count} detail items in database"
    except Exception as e:
        db_logger.error(f"Error updating order {request.task_id}: {str(e)}")
        raise


def handle_order_database_operation(
    request: HttpRequest, response: HttpResponse
):
    """Determine whether to insert or update based on order_id, product_code, and imei
    Handles orders with multiple detail items (combined by maDonHang)"""
    db_logger.info(
        f"Starting handle_order_database_operation for task_id: {request.task_id}"
    )

    # Extract product_codes and imeis from request
    try:
        details_list = request.data["data"][0].get("detail", [])
        if not details_list:
            db_logger.warning(f"No detail items found for task_id: {request.task_id}")
            return insert_order_thread(request, response)
        
        # Check if ANY of the detail items exist in the database
        # If at least one exists, we'll update; otherwise insert
        with db_manager.execute_query() as conn:
            exists_count = 0
            for details in details_list:
                current_product_code = details.get("maHang")
                current_imei = details.get("imei")
                
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) FROM orders
                    WHERE order_id = ? AND product_code = ? AND imei = ?
                """,
                    (request.task_id, current_product_code, current_imei),
                )
                
                if cursor.fetchone()[0] > 0:
                    exists_count += 1

            exists_with_same_identifiers = exists_count > 0

        if exists_with_same_identifiers:
            # Update existing record(s)
            db_logger.info(
                f"Found {exists_count} existing detail items for task_id: {request.task_id}, updating"
            )
            return update_order_thread(request, response)
        else:
            # Insert new record(s)
            db_logger.info(
                f"No existing detail items found for task_id: {request.task_id}, inserting"
            )
            return insert_order_thread(request, response)
    except Exception as e:
        db_logger.error(
            f"Error in handle_order_database_operation for {request.task_id}: {str(e)}"
        )
        raise


def cleanup_old_failed_orders_thread(days: int = 2):
    """Thread function to cleanup orders that failed for more than specified days"""
    db_logger.info(
        f"Starting cleanup_old_failed_orders_thread for orders older than {days} days"
    )

    try:
        with db_manager.execute_query() as conn:
            # Calculate cutoff date (2 days ago by default)
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()

            # Get orders that will be deleted
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM orders
                WHERE status = 'needs_retry' AND updated_at < ?
            """,
                (cutoff_date,),
            )
            count = cursor.fetchone()[0]

            # Delete orders older than retention period
            conn.execute(
                """
                DELETE FROM orders
                WHERE status = 'needs_retry' AND updated_at < ?
            """,
                (cutoff_date,),
            )

        db_logger.info(
            f"Completed cleanup_old_failed_orders_thread - deleted {count} orders older than {days} days"
        )
        return f"Deleted {count} old failed orders from database"
    except Exception as e:
        db_logger.error(f"Error cleaning up old failed orders: {str(e)}")
        raise


def check_retry_window_expired(task_id: str) -> bool:
    """
    Check if the retry window (2 days) has expired for a failed order.
    Returns True if the order should stop being retried.
    """
    try:
        with db_manager.execute_query() as conn:
            cursor = conn.execute(
                """
                SELECT first_failure_time FROM orders
                WHERE order_id = ? AND status = 'needs_retry'
                LIMIT 1
            """,
                (task_id,),
            )
            result = cursor.fetchone()

            if not result or not result[0]:
                # No first_failure_time found, don't expire
                return False

            first_failure_time = datetime.fromisoformat(result[0])
            current_time = datetime.now()
            days_since_first_failure = (
                current_time - first_failure_time
            ).total_seconds() / (24 * 3600)

            if days_since_first_failure >= 2:
                db_logger.info(
                    f"Retry window expired for task_id: {task_id} (failed for {days_since_first_failure:.1f} days)"
                )
                return True

            db_logger.info(
                f"Retry window active for task_id: {task_id} ({days_since_first_failure:.1f}/2 days)"
            )
            return False
    except Exception as e:
        db_logger.error(f"Error checking retry window for {task_id}: {str(e)}")
        # On error, don't expire (safer to keep retrying)
        return False


def delete_order_thread(task_id: str, request: HttpRequest = None):
    """Thread function to delete successful order from database"""
    try:
        with db_manager.execute_query() as conn:
            # Delete from orders table
            conn.execute("DELETE FROM orders WHERE order_id = ?", (task_id,))

            # Delete ALL non_existing_codes associated with this order
            # Multiple codes may have been inserted when the order failed with product code errors
            conn.execute(
                "DELETE FROM non_existing_codes WHERE order_id = ?",
                (task_id,),
            )

        return f"Deleted successful order {task_id} from database"
    except Exception as e:
        db_logger.error(f"Error deleting order {task_id}: {str(e)}")
        raise


async def log_failed_order_to_db(
    ctx: Context,
    request: HttpRequest,
    response: HttpResponse,
    db_path: str = "orders.db",
) -> None:
    """Log failed order information to SQLite database"""

    # Execute database operation durably using Restate's run_typed
    result = await ctx.run_typed(
        "log_failed_order",
        lambda: query_from_thread(
            handle_order_database_operation, request, response
        ),
    )
    return result


async def delete_successful_order_from_db(
    ctx: Context,
    task_id: str,
    request: HttpRequest = None,
    db_path: str = "orders.db",
) -> None:
    """Delete successful order from database"""

    # Execute database operation durably using Restate's run_typed
    result = await ctx.run_typed(
        "delete_successful_order",
        lambda: query_from_thread(delete_order_thread, task_id, request),
    )
    return result


def purge_batch_invocation(invocation_id: str) -> str:
    """
    Purge a batch invocation from Restate storage.
    This removes the invocation metadata and journal when all tasks succeeded.

    Args:
        invocation_id: The ID of the batch invocation to purge

    Returns:
        Status message
    """

    import httpx

    try:
        # Get Restate admin API endpoint from environment or use default
        restate_admin_url = os.getenv(
            "RESTATE_ADMIN_URL", "http://localhost:9070"
        )
        purge_url = f"{restate_admin_url}/invocations/{invocation_id}/purge"

        # Use synchronous httpx client to purge the invocation
        with httpx.Client() as client:
            response = client.patch(purge_url)
            if response.status_code in [200, 202, 204]:
                db_logger.info(
                    f"Successfully purged invocation {invocation_id}"
                )
                return f"Purged invocation {invocation_id}"
            else:
                db_logger.warning(
                    f"Failed to purge invocation {invocation_id}: {response.status_code}"
                )
                return f"Failed to purge invocation {invocation_id}: {response.status_code}"
    except Exception as e:
        db_logger.error(f"Error purging invocation {invocation_id}: {str(e)}")
        raise


@http_task.handler("execute_request")
async def execute_request(ctx: Context, request: HttpRequest) -> Dict[str, Any]:
    """Execute a single HTTP request with error handling and auto-retry"""


    # Check if 2-day retry window has expired
    retry_window_expired = await ctx.run_typed(
        "check_retry_window",
        lambda: check_retry_window_expired(request.task_id),
    )

    if retry_window_expired:
        # Delete the order and stop retrying after 2 days
        db_logger.info(
            f"2-day retry window expired for task_id: {request.task_id}. Deleting order and stopping retries."
        )
        await delete_successful_order_from_db(ctx, request.task_id, request)
        # Return success to stop Restate from retrying
        return HttpResponse(
            task_id=request.task_id,
            url=request.url,
            status_code=0,
            response_data={},
            success=True,
            error="Retry window expired (2 days), order removed from database",
        ).model_dump()

    async def make_http_request():
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    request.url, json=request.data, timeout=30.0
                )

                # Parse response data
                response_data = response.json() if response.content else {}

                # Check for error status codes
                if response.status_code in [
                    400,
                    401,
                    403,
                    404,
                    500,
                    502,
                    503,
                    504,
                ]:
                    # Return failure response - step completes successfully
                    # but we'll process the error and retry the invocation after
                    return HttpResponse(
                        task_id=request.task_id,
                        url=request.url,
                        status_code=response.status_code,
                        response_data=response_data,
                        success=False,
                        error=f"HTTP {response.status_code} error",
                        needs_manual_retry=True,
                    ).model_dump()

                # Success case - return the response
                return HttpResponse(
                    task_id=request.task_id,
                    url=request.url,
                    status_code=response.status_code,
                    response_data=response_data,
                    success=True,
                ).model_dump()

        except Exception as e:
            # Network errors - return failure response
            return HttpResponse(
                task_id=request.task_id,
                url=request.url,
                status_code=0,
                response_data={},
                success=False,
                error=str(e),
                needs_manual_retry=True,
            ).model_dump()

    # Execute the HTTP request durably
    # The http_request step will always complete (either success or failure response)
    response_dict = await ctx.run_typed("http_request", make_http_request)
    # Convert dict back to HttpResponse object
    response = HttpResponse(**response_dict)

    # Handle success case - delete from database and update stats
    if response.success:
        try:
            await delete_successful_order_from_db(ctx, request.task_id, request)
        except Exception as e:
            db_logger.error(
                f"Error deleting order from DB for task_id: {request.task_id}: {str(e)}"
            )
            # Continue anyway - the HTTP request succeeded

        try:
            # Update daily stats for completed task
            await ctx.run_typed(
                "update_daily_stats_completed",
                lambda: query_from_thread(update_daily_stats_thread, True, False),
            )
        except Exception as e:
            db_logger.error(
                f"Error updating daily stats for task_id: {request.task_id}: {str(e)}"
            )
            # Continue anyway - the HTTP request succeeded

        # Return the success response to complete the invocation
        return response.model_dump()

    # Handle failure case - process business logic, then raise exception to retry invocation
    if response.needs_manual_retry:
        # Store API error and extract product codes
        # Only process if errorCode exists (empty errorCode means success)
        error_code = (
            response.response_data.get("errorCode")
            if response.response_data
            else None
        )

        if error_code:  # Only process if errorCode is not empty/None
            # Only process if error_code is a string
            if isinstance(error_code, str):
                # Check for duplicate document pattern first
                duplicate_pattern = r"Chứng từ\s+.+?\s+đã nhập\."
                if re.search(duplicate_pattern, error_code):
                    # Duplicate document - treat as success, delete and complete
                    try:
                        await delete_successful_order_from_db(
                            ctx, request.task_id, request
                        )
                    except Exception as e:
                        db_logger.error(
                            f"Error deleting duplicate order for task_id: {request.task_id}: {str(e)}"
                        )
                        # Continue anyway

                    try:
                        await ctx.run_typed(
                            "update_daily_stats_completed",
                            lambda: query_from_thread(
                                update_daily_stats_thread, True, False
                            ),
                        )
                    except Exception as e:
                        db_logger.error(
                            f"Error updating stats for duplicate task_id: {request.task_id}: {str(e)}"
                        )
                        # Continue anyway

                    db_logger.info(
                        f"Duplicate document detected for task_id: {request.task_id}, "
                        f"treating as success"
                    )
                    # Return success to complete the invocation
                    return HttpResponse(
                        task_id=request.task_id,
                        url=request.url,
                        status_code=response.status_code,
                        response_data=response.response_data,
                        success=True,
                        error=f"Duplicate document detected and removed: {error_code}",
                        needs_manual_retry=False,
                    ).model_dump()

                # Extract non-existing product codes
                pattern = r"Mã hàng\s+([^\s]+(?:\s+[^\s]+)*?)\s+không tồn tại trong hệ thống"
                matches = re.findall(pattern, error_code)

                for code in matches:
                    query_from_thread(
                        insert_non_existing_code_thread, code, request.task_id
                    )
                    # Add to cache set (automatically handles duplicates)
                    non_existing_codes_cache.add(code)

                # Ensure proper UTF-8 encoding
                try:
                    error_code = (
                        error_code.encode("utf-8")
                        .decode("unicode_escape")
                        .encode("latin1")
                        .decode("utf-8")
                    )
                except (UnicodeDecodeError, UnicodeEncodeError):
                    pass

            # Log failed order to database
            await log_failed_order_to_db(ctx, request, response)
        else:
            # Empty errorCode means successful HTTP request - treat as success
            # Delete the successful order from database
            try:
                await delete_successful_order_from_db(ctx, request.task_id, request)
            except Exception as e:
                db_logger.error(
                    f"Error deleting order for empty errorCode task_id: {request.task_id}: {str(e)}"
                )
                # Continue anyway

            try:
                await ctx.run_typed(
                    "update_daily_stats_completed",
                    lambda: query_from_thread(
                        update_daily_stats_thread, True, False
                    ),
                )
            except Exception as e:
                db_logger.error(
                    f"Error updating stats for empty errorCode task_id: {request.task_id}: {str(e)}"
                )
                # Continue anyway

            db_logger.info(
                f"Empty errorCode for task_id: {request.task_id}, treating as success"
            )
            # Return success to complete the invocation
            return response.model_dump()

        # Update daily stats for failed task
        await ctx.run_typed(
            "update_daily_stats_failed",
            lambda: query_from_thread(update_daily_stats_thread, False, True),
        )

        # Determine if this error is retryable or terminal
        # Client errors (4xx) are terminal - the request is invalid and won't succeed on retry
        # Server errors (5xx) are retryable - the server may recover
        if response.status_code in [400, 401, 403, 404]:
            # Terminal error - do not retry
            db_logger.warning(
                f"HttpTask failed with terminal error for task_id: {request.task_id}, "
                f"status_code: {response.status_code}, not retrying (client error)"
            )
            raise TerminalError(
                f"HTTP request failed: {response.error} (status_code: {response.status_code})"
            )
        else:
            # Retryable error - raise regular exception to trigger retry
            # according to InvocationRetryPolicy (15-minute intervals)
            db_logger.warning(
                f"HttpTask failed for task_id: {request.task_id}, "
                f"status_code: {response.status_code}, raising exception to retry invocation"
            )
            raise Exception(
                f"HTTP request failed: {response.error} (status_code: {response.status_code})"
            )

    # Should never reach here - all paths above either return or raise
    db_logger.error(
        f"Unexpected code path for task_id: {request.task_id}, response: {response}"
    )
    raise Exception("Unexpected error in execute_request handler")


# Service for batch management
batch_service = restate.Service("BatchService")


def generate_excel_file(codes: list) -> str:
    """Generate Excel file with non-existing codes and return filename"""
    from openpyxl.styles import Border, Font, PatternFill, Side

    # Create DataFrame with codes
    df = pd.DataFrame(
        {
            "Product Code": codes,
            "Status": ["Not Found"] * len(codes),
            "Detected At": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            * len(codes),
            "Action Required": ["Verify & Add to System"] * len(codes),
        }
    )

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"non_existing_codes_{timestamp}.xlsx"

    # Create Excel file with formatting
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Non-Existing Codes", index=False)

        # Get workbook and worksheet objects for openpyxl
        workbook = writer.book
        worksheet = writer.sheets["Non-Existing Codes"]

        # Define styles
        header_font = Font(bold=True)
        header_fill = PatternFill(
            start_color="D7E4BC", end_color="D7E4BC", fill_type="solid"
        )
        border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin"),
        )

        # Format header row
        for col in range(1, len(df.columns) + 1):
            cell = worksheet.cell(row=1, column=col)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = border

        # Format data cells and add borders
        for row in range(2, len(df) + 2):
            for col in range(1, len(df.columns) + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.border = border

        # Adjust column widths
        worksheet.column_dimensions["A"].width = 15  # Product Code
        worksheet.column_dimensions["B"].width = 12  # Status
        worksheet.column_dimensions["C"].width = 20  # Detected At
        worksheet.column_dimensions["D"].width = 25  # Action Required

    return filename


def send_non_existing_codes_email_sync(codes: list = None) -> dict:
    """
    Send email with the list of non-existing codes and Excel attachment.

    Args:
        codes: List of codes to send. If None, fetches unsent codes from database.

    Returns:
        dict with status and count of codes sent
    """
    global non_existing_codes_cache

    try:
        import smtplib
        from email import encoders
        from email.mime.base import MIMEBase
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        # If no codes provided, fetch unsent codes from database
        if codes is None:
            codes = get_unsent_non_existing_codes_thread()

        if not codes:
            db_logger.info("No unsent codes to email")
            return {"status": "no_codes", "count": 0}

        # Email configuration from environment variables
        smtp_server = os.getenv("SMTP_SERVER")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        email_address = os.getenv("EMAIL_ADDRESS")
        email_password = os.getenv("EMAIL_PASSWORD")
        recipients = os.getenv(
            "EMAIL_RECIPIENTS", "nam.nguyen@lug.vn,songkhoi123@gmail.com"
        )

        # Generate Excel file
        excel_filename = generate_excel_file(codes)

        # Create message
        msg = MIMEMultipart()
        msg["From"] = email_address
        msg["To"] = recipients
        msg["Subject"] = (
            f"MÃ ĐƠN HÀNG CÒN THIẾU - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )

        # Create email body
        body = f"""
            Thời gian xử lý: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

            Tổng số mã hàng không tồn tại trong hệ thống: {len(codes)}

            Chi tiết danh sách mã hàng được đính kèm trong file Excel.

            Vui lòng kiểm tra file đính kèm để xem danh sách đầy đủ.
        """
        msg.attach(MIMEText(body, "plain"))

        # Attach Excel file
        with open(excel_filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {excel_filename}",
        )
        msg.attach(part)

        # Send email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(email_address, email_password)
        server.sendmail(
            email_address,
            recipients.split(","),
            msg.as_string(),
        )
        server.quit()

        # Mark codes as sent in database AFTER successful email
        mark_codes_as_sent_thread(codes)

        # Clear in-memory cache
        non_existing_codes_cache.clear()

        # Clean up Excel file after sending
        try:
            os.remove(excel_filename)
        except FileNotFoundError:
            pass

        db_logger.info(
            f"Email sent successfully with {len(codes)} non-existing codes"
        )
        return {"status": "sent", "count": len(codes)}

    except Exception as e:
        db_logger.error(f"Failed to send email: {e}")
        # Clean up Excel file if email fails
        try:
            excel_filename = f"non_existing_codes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            if os.path.exists(excel_filename):
                os.remove(excel_filename)
        except:
            pass
        return {"status": "failed", "error": str(e), "count": 0}


@batch_service.handler("submit_batch")
async def submit_batch(ctx: Context, requests: list) -> Dict[str, str]:
    """
    Submit a batch of HTTP requests sequentially (one at a time).

    Processes each request in order, waiting for each to complete before starting the next.
    This prevents overloading the target server with parallel requests.

    Args:
        requests: List of HTTP request data (batch of requests to process)

    Returns:
        Status dict with task IDs and any errors encountered
    """
    task_ids = []
    errors = []
    successful_count = 0

    # Process requests sequentially (one at a time)
    for req_data in requests:
        # Generate unique task_id using hash of the entire request data
        # This ensures each order item gets a unique ID even with same maDonHang
        try:
            # Create a deterministic hash from the request data
            request_json = json.dumps(req_data, sort_keys=True)
            task_id = hashlib.md5(request_json.encode()).hexdigest()
        except (KeyError, IndexError, TypeError) as e:
            raise ValueError(f"Failed to generate task_id from request data: {e}")

        request = HttpRequest(
            url=req_data["url"], data=req_data["data"], task_id=task_id
        )
        task_ids.append(task_id)

        # Execute request sequentially - await immediately before processing next request
        try:
            response_dict = await ctx.service_call(execute_request, request)

            # Check success from dict directly
            if response_dict.get("success", False):
                successful_count += 1
            else:
                errors.append(
                    {
                        "task_id": task_id,
                        "error": response_dict.get("error", "Unknown error"),
                        "status_code": response_dict.get("status_code", 0),
                        "response_data": response_dict.get("response_data", {}),
                    }
                )
        except Exception as e:
            # Catch any unexpected errors during service call
            db_logger.error(f"Error executing request for task_id {task_id}: {str(e)}")
            errors.append(
                {
                    "task_id": task_id,
                    "error": str(e),
                    "status_code": 0,
                    "response_data": {},
                }
            )

    # Send email with non-existing codes immediately after processing all orders
    db_logger.info("Checking for non-existing codes to email...")
    email_result = await ctx.run_typed(
        "send_non_existing_codes_email",
        lambda: query_from_thread(send_non_existing_codes_email_sync, None),
    )

    if email_result["status"] == "sent":
        db_logger.info(f"Email sent with {email_result['count']} non-existing codes")
    elif email_result["status"] == "no_codes":
        db_logger.info("No non-existing codes to email")
    else:
        db_logger.error(f"Failed to send non-existing codes email: {email_result.get('error', 'unknown')}")

    # Return response with processing results
    batch_status_message = ""
    if not errors:
        batch_status_message = "All tasks completed successfully"
    else:
        batch_status_message = (
            f"{successful_count} succeeded, {len(errors)} failed - failed tasks will retry according to retry policy"
        )

    return {
        "message": f"Processed {len(task_ids)} tasks sequentially",
        "task_ids": task_ids,
        "successful_count": successful_count,
        "failed_count": len(errors),
        "errors": errors if errors else None,
        "batch_status": "all_succeeded" if not errors else "some_failed",
        "status_details": batch_status_message,
        "email_result": email_result,
    }


@batch_service.handler("send_single_email")
async def send_single_email(ctx: Context) -> Dict[str, str]:
    """Send a single email with non-existing codes from database"""
    # Send email using database as source of truth
    result = await ctx.run_typed(
        "send_non_existing_codes_email",
        lambda: query_from_thread(send_non_existing_codes_email_sync, None),
    )

    if result["status"] == "sent":
        return {
            "message": f"Email sent with {result['count']} codes"
        }
    elif result["status"] == "no_codes":
        return {"message": "No unsent codes to send"}
    else:
        return {"message": f"Email failed: {result.get('error', 'unknown error')}"}




@batch_service.handler("cleanup_old_failed_orders")
async def cleanup_old_failed_orders(
    ctx: Context, days: int = 2
) -> Dict[str, str]:
    """
    Cleanup orders that have failed and are older than the retention period.
    This handler should be called periodically (e.g., once per day) to clean up
    old failed invocations that are no longer being retried.

    Args:
        days: Number of days to retain failed orders (default: 2)

    Returns:
        Status message with count of deleted orders
    """
    result = await ctx.run_typed(
        "cleanup_failed_orders",
        lambda: query_from_thread(cleanup_old_failed_orders_thread, days),
    )
    return {"message": result}


application = app(services=[batch_service, http_task])

if __name__ == "__main__":
    import hypercorn.asyncio
    import hypercorn.config

    config = hypercorn.config.Config()
    config.bind = ["0.0.0.0:9080"]
    config.alpn_protocols = ["http/1.1"]
    # Disable Hypercorn's default logging to prevent duplicate logs
    # Our logging is already configured via logging.basicConfig()
    config.accesslog = None  # Disable access logs
    config.errorlog = None   # Disable error logs (prevents duplicate logging)

    asyncio.run(hypercorn.asyncio.serve(application, config))
