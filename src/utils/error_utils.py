from typing import Type, Callable, Any, Optional
from functools import wraps
import time
import random
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    Retrying,
    RetryError
)
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)

class CircuitBreaker:
    """Circuit breaker pattern implementation."""

    def __init__(self, threshold: int = 5, reset_timeout: float = 60.0) -> None:
        """Initialize circuit breaker.

        Args:
            threshold: Number of failures before opening circuit
            reset_timeout: Time in seconds before resetting circuit
        """
        self.threshold = threshold
        self.reset_timeout = reset_timeout
        self.failures = {}
        self.last_failure_time = None

    def record_failure(self, operation: str, error: Exception) -> None:
        """Record a failure for the given operation.

        Args:
            operation: Name of the operation that failed
            error: The error that occurred
        """
        if operation not in self.failures:
            self.failures[operation] = 0
        self.failures[operation] += 1
        self.last_failure_time = time.time()
        logger.logjson(
            "WARNING",
            f"Circuit breaker failure for {operation}",
            {"error": str(error), "failures": self.failures[operation]},
        )
        if self.failures[operation] >= self.threshold:
            logger.logjson(
                "WARNING",
                "Circuit breaker opened",
                {"failures": self.failures[operation], "threshold": self.threshold},
            )

    def record_success(self, operation: str) -> None:
        """Record a success for the given operation.

        Args:
            operation: Name of the operation that succeeded
        """
        if operation in self.failures:
            del self.failures[operation]
        self.last_failure_time = None

    def is_closed(self, operation: str) -> bool:
        """Check if circuit is closed for the given operation.

        Args:
            operation: Name of the operation to check

        Returns:
            bool: True if circuit is closed, False if open
        """
        if operation not in self.failures:
            return True

        if self.failures[operation] < self.threshold:
            return True

        # If we've passed the reset timeout, allow one request through
        if self.last_failure_time and (time.time() - self.last_failure_time) > self.reset_timeout:
            self.reset()
            return True

        return False

    def reset(self) -> None:
        """Reset the circuit breaker state."""
        self.failures.clear()
        self.last_failure_time = None

def with_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
    circuit_breaker: Optional[CircuitBreaker] = None
) -> Callable:
    """Retry decorator with exponential backoff and circuit breaker."""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Check circuit breaker
            if circuit_breaker and not circuit_breaker.is_closed(func.__name__):
                raise Exception("Circuit breaker is open")

            def _execute_with_retry():
                try:
                    result = func(*args, **kwargs)
                    if circuit_breaker:
                        circuit_breaker.record_success(func.__name__)
                    return result
                except Exception as e:
                    if circuit_breaker:
                        circuit_breaker.record_failure(func.__name__, e)
                    raise

            retry_config = Retrying(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=base_delay, max=max_delay),
                retry=retry_if_exception_type((Exception,)),
                reraise=True,
                before_sleep=before_sleep_log(logger, log_level=20)
            )

            try:
                return retry_config(_execute_with_retry)
            except RetryError as e:
                # Preserve the original error
                if e.last_attempt.exception():
                    raise e.last_attempt.exception()
                raise

        return wrapper
    return decorator

def handle_database_error(func: Callable) -> Callable:
    """
    Decorator for handling database-specific errors
    
    Args:
        func: Function to decorate
        
    Returns:
        Callable: Decorated function
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_msg = str(e).lower()
            
            # Handle connection errors
            if "connection" in error_msg or "timeout" in error_msg:
                logger.logjson("ERROR", "Database connection error", {
                    "error": str(e),
                    "function": func.__name__
                })
                raise ConnectionError(f"Database connection error: {str(e)}")
                
            # Handle query errors
            elif "syntax" in error_msg or "invalid" in error_msg:
                logger.logjson("ERROR", "Invalid query", {
                    "error": str(e),
                    "function": func.__name__
                })
                raise ValueError(f"Invalid query: {str(e)}")
                
            # Handle permission errors
            elif "permission" in error_msg or "access" in error_msg:
                logger.logjson("ERROR", "Permission denied", {
                    "error": str(e),
                    "function": func.__name__
                })
                raise PermissionError(f"Permission denied: {str(e)}")
                
            # Handle other errors
            else:
                logger.logjson("ERROR", "Unexpected database error", {
                    "error": str(e),
                    "function": func.__name__
                })
                raise
                
    return wrapper 