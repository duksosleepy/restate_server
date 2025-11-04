import asyncio
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, Optional

import httpx
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel
import restate
from restate import (
    Context,
    ObjectContext,
    Service,
    VirtualObject,
    app,
    InvocationRetryPolicy
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
        """Initialize database tables if they don't exist"""
        with self.execute_query() as conn:
            # Create orders table
            conn.execute("select(1);")

            conn.commit()

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


http_task = VirtualObject(
    "HttpTask",
    inactivity_timeout=timedelta(days=30),
    invocation_retry_policy=InvocationRetryPolicy(
        initial_interval=timedelta(minutes=15),
        exponentiation_factor=2.0,
        max_interval=None,  # No upper limit
        max_attempts=None,  # Retry indefinitely
        on_max_attempts=None
    )
)

non_existing_codes = []


def query_from_thread(query_func, *args):
    """Execute a database query in a thread-safe manner"""
    db_logger.info(
        f"Executing database query with function: {query_func.__name__}"
    )
    with sqlite_lock:
        try:
            result = query_func(*args)
            db_logger.info(
                f"Database query completed successfully: {query_func.__name__}"
            )
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

        db_logger.info(
            f"Updated daily stats - completed: {task_completed}, failed: {task_failed}"
        )
    except Exception as e:
        db_logger.error(f"Error updating daily stats: {str(e)}")
        raise


def insert_non_existing_code_thread(product_code: str, order_id: str):
    """Thread function to insert non-existing product code into database"""
    db_logger.info(
        f"Inserting non-existing code {product_code} for order {order_id}"
    )

    try:
        with db_manager.execute_query() as conn:
            # Use INSERT OR IGNORE to handle duplicates gracefully
            conn.execute(
                """
                INSERT OR IGNORE INTO non_existing_codes (product_code, order_id)
                VALUES (?, ?)
            """,
                (product_code, order_id),
            )

            # Check if the insert was successful
            if conn.total_changes > 0:
                db_logger.info(
                    f"Inserted non-existing code {product_code} for order {order_id}"
                )
            else:
                db_logger.info(
                    f"Non-existing code {product_code} already exists in database"
                )
    except Exception as e:
        db_logger.error(
            f"Error inserting non-existing code {product_code}: {str(e)}"
        )
        raise


def insert_order_thread(request: HttpRequest, response: HttpResponse):
    """Thread function to insert new order in database"""
    db_logger.info(
        f"Starting insert_order_thread for task_id: {request.task_id}"
    )

    # Extract order data from the request
    try:
        order_data = request.data["data"][0]["master"]
        details = request.data["data"][0].get("detail", [{}])[0]
    except (KeyError, IndexError):
        order_data = {}
        details = {}

    # Determine source_type based on maDonHang
    ma_don_hang = order_data.get("maDonHang", "")
    source_type = "offline" if "/" in ma_don_hang else "online"

    try:
        with db_manager.execute_query() as conn:
            # Use INSERT OR REPLACE to handle duplicates
            conn.execute(
                """
                INSERT OR REPLACE INTO orders (
                    order_id, customer_name, phone_number, document_type, document_number,
                    department_code, order_date, province, district, ward, address,
                    product_code, product_name, imei, quantity, revenue, source_type,
                    status, error_code, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    datetime.now().isoformat(),  # updated_at
                ),
            )

        db_logger.info(
            f"Completed insert_order_thread for task_id: {request.task_id}"
        )
        return f"Inserted new order {request.task_id} to database"
    except Exception as e:
        db_logger.error(f"Error inserting order {request.task_id}: {str(e)}")
        raise


def update_order_thread(request: HttpRequest, response: HttpResponse):
    """Thread function to update existing order based on order_id, product_code, and imei"""
    db_logger.info(
        f"Starting update_order_thread for task_id: {request.task_id}"
    )

    # Extract order data from the request
    try:
        order_data = request.data["data"][0]["master"]
        details = request.data["data"][0].get("detail", [{}])[0]
    except (KeyError, IndexError):
        order_data = {}
        details = {}

    # Determine source_type based on maDonHang
    ma_don_hang = order_data.get("maDonHang", "")
    source_type = "offline" if "/" in ma_don_hang else "online"

    try:
        with db_manager.execute_query() as conn:
            # Update existing record
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

        db_logger.info(
            f"Completed update_order_thread for task_id: {request.task_id}"
        )
        return f"Updated existing order {request.task_id} in database"
    except Exception as e:
        db_logger.error(f"Error updating order {request.task_id}: {str(e)}")
        raise


def handle_order_database_operation(
    request: HttpRequest, response: HttpResponse
):
    """Determine whether to insert or update based on order_id, product_code, and imei"""
    db_logger.info(
        f"Starting handle_order_database_operation for task_id: {request.task_id}"
    )

    # Extract product_code and imei from request
    try:
        details = request.data["data"][0].get("detail", [{}])[0]
        current_product_code = details.get("maHang")
        current_imei = details.get("imei")
    except (KeyError, IndexError):
        current_product_code = None
        current_imei = None

    try:
        with db_manager.execute_query() as conn:
            # Check if record exists with same order_id, product_code, and imei
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM orders
                WHERE order_id = ? AND product_code = ? AND imei = ?
            """,
                (request.task_id, current_product_code, current_imei),
            )

            exists_with_same_identifiers = cursor.fetchone()[0] > 0

        if exists_with_same_identifiers:
            # Update existing record
            return update_order_thread(request, response)
        else:
            # Insert new record
            return insert_order_thread(request, response)
    except Exception as e:
        db_logger.error(
            f"Error in handle_order_database_operation for {request.task_id}: {str(e)}"
        )
        raise


def delete_order_thread(task_id: str, request: HttpRequest = None):
    """Thread function to delete successful order from database"""
    db_logger.info(f"Starting delete_order_thread for task_id: {task_id}")

    try:
        with db_manager.execute_query() as conn:
            # Delete from orders table
            conn.execute("DELETE FROM orders WHERE order_id = ?", (task_id,))

            # If request data is available, also delete from non_existing_codes table
            if request:
                try:
                    details = request.data["data"][0].get("detail", [{}])[0]
                    product_code = details.get("maHang")
                    if product_code:
                        db_logger.info(
                            f"Deleting product code {product_code} from non_existing_codes table"
                        )
                        conn.execute(
                            "DELETE FROM non_existing_codes WHERE product_code = ?",
                            (product_code,),
                        )
                        db_logger.info(
                            f"Deleted product code {product_code} from non_existing_codes table"
                        )
                except (KeyError, IndexError) as e:
                    db_logger.warning(
                        f"Could not extract product code from request for task_id {task_id}: {e}"
                    )

        db_logger.info(f"Completed delete_order_thread for task_id: {task_id}")
        return f"Deleted successful order {task_id} from database"
    except Exception as e:
        db_logger.error(f"Error deleting order {task_id}: {str(e)}")
        raise


async def log_failed_order_to_db(
    ctx: ObjectContext,
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
    ctx: ObjectContext,
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


@http_task.handler("execute_request")
async def execute_request(
    ctx: ObjectContext, request: HttpRequest
) -> HttpResponse:
    """Execute a single HTTP request with error handling and auto-retry"""

    async def make_http_request():
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    request.url, json=request.data, timeout=30.0
                )

                # Check for error status codes that need retry
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
                    return HttpResponse(
                        task_id=request.task_id,
                        url=request.url,
                        status_code=response.status_code,
                        response_data=response.json()
                        if response.content
                        else {},
                        success=False,
                        error=f"HTTP {response.status_code} error",
                        needs_manual_retry=True,
                    ).model_dump()

                return HttpResponse(
                    task_id=request.task_id,
                    url=request.url,
                    status_code=response.status_code,
                    response_data=response.json(),
                    success=True,
                ).model_dump()

        except Exception as e:
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
    response_dict = await ctx.run_typed(
        "http_request", make_http_request
    )
    # Convert dict back to HttpResponse object
    response = HttpResponse(**response_dict)

    # Handle success case - delete from database and update stats
    if response.success:
        await delete_successful_order_from_db(ctx, request.task_id, request)
        # Update daily stats for completed task
        await ctx.run_typed(
            "update_daily_stats_completed",
            lambda: query_from_thread(update_daily_stats_thread, True, False),
        )
        return response

    # Handle failure case - log to database and schedule retry
    if response.needs_manual_retry:
        # Store API error and extract product codes
        if response.response_data and response.response_data.get("errorCode"):
            error_code = response.response_data.get("errorCode")

            # Only process if error_code is a string
            if isinstance(error_code, str):
                # Check for duplicate document pattern first
                duplicate_pattern = r"Chứng từ\s+.+?\s+đã nhập\."
                if re.search(duplicate_pattern, error_code):
                    # Delete the task instead of retrying
                    await delete_successful_order_from_db(ctx, request.task_id, request)
                    await ctx.run_typed(
                        "update_daily_stats_completed",
                        lambda: query_from_thread(update_daily_stats_thread, True, False),
                    )
                    return HttpResponse(
                        task_id=request.task_id,
                        url=request.url,
                        status_code=response.status_code,
                        response_data=response.response_data,
                        success=True,
                        error=f"Duplicate document detected and removed: {error_code}",
                        needs_manual_retry=False,
                    )

                # Extract non-existing product codes
                pattern = r"Mã hàng\s+([^\s]+(?:\s+[^\s]+)*?)\s+không tồn tại trong hệ thống"
                matches = re.findall(pattern, error_code)

                for code in matches:
                    query_from_thread(
                        insert_non_existing_code_thread, code, request.task_id
                    )
                    if code not in non_existing_codes:
                        non_existing_codes.append(code)

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

        # Update daily stats for failed task
        await ctx.run_typed(
            "update_daily_stats_failed",
            lambda: query_from_thread(update_daily_stats_thread, False, True),
        )

    return response


# Service for batch management
batch_service = restate.Service("BatchService")

# Global variables for email timing
email_scheduler_active = False
first_submit_time = None


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


async def send_non_existing_codes_email(codes: list) -> None:
    """Send email with the list of existing codes and Excel attachment"""
    try:
        import smtplib
        from email import encoders
        from email.mime.base import MIMEBase
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        # Email configuration from environment variables
        smtp_server = os.getenv("SMTP_SERVER")
        smtp_port = int(os.getenv("SMTP_PORT"))
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
        if codes:
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

        non_existing_codes.clear()

        # Clean up Excel file after sending
        try:
            os.remove(excel_filename)
        except FileNotFoundError:
            pass

        print(
            f"Email sent successfully with {len(codes)} existing codes and Excel attachment"
        )

    except Exception as e:
        print(f"Failed to send email: {e}")
        # Clean up Excel file if email fails
        try:
            excel_filename = f"non_existing_codes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            if os.path.exists(excel_filename):
                os.remove(excel_filename)
        except:
            pass


@batch_service.handler("submit_batch")
async def submit_batch(ctx: Context, requests: list) -> Dict[str, str]:
    """Submit a batch of HTTP requests and wait for quick errors (5 second timeout)"""
    global email_scheduler_active, first_submit_time

    task_ids = []
    request_futures = []

    # Set first submit time and start email scheduler if not already active
    if not email_scheduler_active:
        first_submit_time = asyncio.get_event_loop().time()
        email_scheduler_active = True

        # Schedule single email after 5 minutes (fire-and-forget)
        ctx.service_send(send_single_email, arg={}, send_delay=timedelta(minutes=5))

    # Start all requests in parallel without blocking
    for req_data in requests:
        # Use Ma_Don_Hang from the request data as task ID
        try:
            task_id = req_data["data"]["data"][0]["master"]["maDonHang"]
        except (KeyError, IndexError, TypeError):
            raise ValueError("Ma_Don_Hang is required for task identification")

        request = HttpRequest(
            url=req_data["url"], data=req_data["data"], task_id=task_id
        )

        # Start request processing asynchronously (don't await yet)
        future = ctx.object_call(execute_request, task_id, request)
        request_futures.append((task_id, future))
        task_ids.append(task_id)

    # Wait for quick errors with 5 second timeout
    errors = []
    timeout = timedelta(seconds=5)

    try:
        # Use select to wait with timeout
        match await restate.select(
            results=restate.gather(*[f for _, f in request_futures]),
            timeout=ctx.sleep(timeout)
        ):
            case ["results", responses]:
                # All completed within timeout - check for errors
                for (task_id, _), response in zip(request_futures, responses):
                    if not response.success:
                        errors.append({
                            "task_id": task_id,
                            "error": response.error,
                            "status_code": response.status_code,
                            "response_data": response.response_data,
                        })
            case ["timeout", _]:
                # Timeout occurred - some requests still processing in background
                # Collect any responses that are available
                for task_id, future in request_futures:
                    try:
                        response = await future
                        if not response.success:
                            errors.append({
                                "task_id": task_id,
                                "error": response.error,
                                "status_code": response.status_code,
                                "response_data": response.response_data,
                            })
                    except Exception:
                        # Request still processing, will continue in background
                        pass
    except Exception as e:
        db_logger.error(f"Error in batch submission timeout handling: {str(e)}")

    # Return response with any immediate errors, rest continue in background
    return {
        "message": f"Submitted {len(task_ids)} tasks for processing",
        "task_ids": task_ids,
        "errors": errors if errors else None,
        "info": "Remaining requests are being processed asynchronously in the background"
    }


@batch_service.handler("send_single_email")
async def send_single_email(ctx: Context) -> Dict[str, str]:
    """Send a single email with existing codes"""
    global non_existing_codes, email_scheduler_active

    if non_existing_codes:
        # Send email with current codes
        codes_to_send = non_existing_codes.copy()
        await send_non_existing_codes_email(codes_to_send)

        # Clear only the in-memory list, not the database
        non_existing_codes.clear()

        # Stop the email scheduler after sending
        email_scheduler_active = False

        return {
            "message": f"Email sent with {len(codes_to_send)} codes, scheduler stopped"
        }
    else:
        # No codes to send, stop the email scheduler
        email_scheduler_active = False
        return {"message": "No codes to send, email scheduler stopped"}


@batch_service.handler("stop_email_scheduler")
async def stop_email_scheduler(ctx: Context) -> Dict[str, str]:
    """Stop the email scheduler"""
    global email_scheduler_active, first_submit_time

    email_scheduler_active = False
    first_submit_time = None

    return {"message": "Email scheduler stopped"}


application = app(services=[batch_service, http_task])

if __name__ == "__main__":
    import hypercorn.asyncio
    import hypercorn.config

    config = hypercorn.config.Config()
    config.bind = ["0.0.0.0:9080"]
    config.alpn_protocols = ["http/1.1"]

    asyncio.run(hypercorn.asyncio.serve(application, config))
