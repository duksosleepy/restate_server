import asyncio
from datetime import timedelta
from typing import Any, Dict, Optional

import httpx
from pydantic import BaseModel
from restate import (
    Context,
    ObjectContext,
    RunOptions,
    Service,
    VirtualObject,
    app,
)
from restate.exceptions import TerminalError


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


class TaskStatus(BaseModel):
    task_id: str
    status: str  # "pending", "running", "completed", "failed", "needs_retry"
    response: Optional[HttpResponse] = None
    retry_count: int = 0


# Virtual Object for managing individual HTTP request tasks
http_task = VirtualObject("HttpTask")


@http_task.handler()
async def execute_request(
    ctx: ObjectContext, request: HttpRequest
) -> HttpResponse:
    """Execute a single HTTP request with error handling and auto-retry"""

    # Get current task status with proper type hint
    task_status = await ctx.get("status", type_hint=TaskStatus) or TaskStatus(
        task_id=request.task_id, status="pending", retry_count=0
    )

    # Update status to running
    task_status.status = "running"
    ctx.set("status", task_status)

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
                    )

                return HttpResponse(
                    task_id=request.task_id,
                    url=request.url,
                    status_code=response.status_code,
                    response_data=response.json(),
                    success=True,
                )

        except Exception as e:
            return HttpResponse(
                task_id=request.task_id,
                url=request.url,
                status_code=0,
                response_data={},
                success=False,
                error=str(e),
                needs_manual_retry=True,
            )

    # Execute the HTTP request durably
    response = await ctx.run_typed(
        "http_request", make_http_request, RunOptions(type_hint=HttpResponse)
    )

    # Handle response based on success/failure
    if response.needs_manual_retry:
        # Increment retry count
        task_status.retry_count += 1
        task_status.status = "needs_retry"
        task_status.response = response
        ctx.set("status", task_status)

        # Schedule automatic retry after delay (exponential backoff)

        retry_delay = min(
            300, 5 * (2 ** min(task_status.retry_count, 6))
        )  # Max 5 minutes

        # Schedule retry using delayed self-invocation
        ctx.object_send(
            execute_request,
            ctx.key(),
            request,
            send_delay=timedelta(seconds=retry_delay),
        )

        return response
    else:
        # Task completed successfully - clean up state
        ctx.clear_all()  # Remove all state for this task
        return response


@http_task.handler()
async def retry_task(ctx: ObjectContext, request: HttpRequest) -> HttpResponse:
    """Manually retry a failed task"""

    # Get current task status with proper type hint
    task_status = await ctx.get("status", type_hint=TaskStatus)
    if not task_status:
        raise TerminalError(f"Task {request.task_id} not found")

    # Increment retry count
    task_status.retry_count += 1
    task_status.status = "running"
    ctx.set("status", task_status)

    # Re-execute the request
    return await execute_request(ctx, request)


@http_task.handler()
async def get_task_status(ctx: ObjectContext) -> Optional[TaskStatus]:
    """Get the current status of a task"""
    return await ctx.get("status", type_hint=TaskStatus)


# Service for batch management
batch_service = Service("BatchService")


@batch_service.handler()
async def submit_batch(ctx: Context, requests: list) -> Dict[str, str]:
    """Submit a batch of HTTP requests as individual tasks"""

    task_ids = []
    for req_data in requests:
        # Use Ma_Don_Hang from the request data as task ID
        # Extract from the nested structure: data.data[0].master.maDonHang
        try:
            task_id = req_data["data"]["data"][0]["master"]["maDonHang"]
        except (KeyError, IndexError, TypeError):
            # If Ma_Don_Hang is not available, generate a fallback ID
            # You might want to handle this differently based on your requirements
            raise ValueError("Ma_Don_Hang is required for task identification")

        request = HttpRequest(
            url=req_data["url"], data=req_data["data"], task_id=task_id
        )

        # Submit each request as a separate task (fire and forget)
        ctx.object_send(execute_request, task_id, request)
        task_ids.append(task_id)

    return {"message": f"Submitted {len(task_ids)} tasks", "task_ids": task_ids}


application = app(services=[batch_service, http_task])

if __name__ == "__main__":
    import hypercorn.asyncio
    import hypercorn.config

    config = hypercorn.config.Config()
    config.bind = ["0.0.0.0:9080"]

    asyncio.run(hypercorn.asyncio.serve(application, config))
