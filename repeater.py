import sys
from time import sleep
import urllib.error


class RepeaterException(Exception):
    def __init__(self, message, errors=None):
        self.message = message
        self.errors = errors

    def __str__(self):
        return self.message


class RepeaterDecoratorClass:
    """
        Repeats attempts run func with increasing timeout (seconds)
    """
    DEFAULT_TIMEOUTS = (10, 30, 60, 120, 300, 300, 300, 300)
    RETRY_ON_EXCEPTIONS_DEFAULTS = (
        urllib.error.URLError,
        urllib.error.HTTPError,
        urllib.error.ContentTooShortError,
        ConnectionError,
    )

    def __init__(
            self,
            timeouts: tuple = None,
            retry_on: tuple = None,
            exit_on_error: bool = False,
            logger=None
    ):
        self.timeouts = timeouts or self.DEFAULT_TIMEOUTS
        self.retry_on_exceptions = retry_on or self.RETRY_ON_EXCEPTIONS_DEFAULTS
        self.exit_on_error = exit_on_error
        self.logger = logger

    def __call__(self, func):
        def wrapped_func(*args, **kwargs):
            last_err = None
            idx = 0

            for idx, timeout in enumerate(self.timeouts):
                try:
                    return func(*args, **kwargs)
                except self.retry_on_exceptions as e:
                    last_err = e
                    msg = f"Repeater, expected error: {e!s}"
                    self.logger.debug(msg) if self.logger else print(msg)
                    sleep(timeout)
                    continue
                except Exception as e:
                    last_err = e
                    msg = f"Unexpected error: {e!s}"
                    self.logger.exception(msg) if self.logger else print(msg)
                    break

            msg = f"Task failed after {idx + 1} retry with error: {last_err!s}"
            self.logger.exception(msg) if self.logger else print(msg)

            if self.exit_on_error:
                sys.exit(1)
            else:
                raise RepeaterException(msg)

        return wrapped_func


repeater = RepeaterDecoratorClass
