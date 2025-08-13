import asyncio
import functools
from typing import Any, Awaitable, Callable, Tuple, Type, TypeVar

T = TypeVar("T")


def retry_async(
    retries: int = 3,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    delay: float | None = None,
    delay_manager: "DelayManager | None" = None,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Retry decorator for asynchronous functions.

    Parameters
    ----------
    retries:
        Number of attempts before the exception is reraised.
    exceptions:
        A tuple of exception classes that trigger a retry.
    delay:
        Optional fixed delay between attempts (seconds). Ignored when a
        ``delay_manager`` is provided.
    delay_manager:
        Instance of :class:`DelayManager` used to control waiting between
        retries.  Only the ``wait_error`` method is used.
    """

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions:  # type: ignore[misc]
                    attempt += 1
                    if attempt >= retries:
                        raise
                    if delay_manager is not None:
                        await delay_manager.wait_error()
                    elif delay is not None:
                        await asyncio.sleep(delay)
        return wrapper

    return decorator


# Avoid circular import type checking issues
try:  # pragma: no cover - optional import only used for typing
    from .delay_manager import DelayManager  # noqa: E402  # pylint: disable=unused-import
except Exception:  # pragma: no cover
    DelayManager = None  # type: ignore
