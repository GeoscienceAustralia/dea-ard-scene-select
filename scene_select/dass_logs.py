"""
Logging configuration for wagl logs

Defines structured logging for:
    * Task message      -- qualname task
    * Status messages   -- qualname status
    * Luigi interface   -- qualname luigi-interface
"""

import functools
import logging
import traceback

import structlog
from structlog.processors import JSONRenderer

COMMON_PROCESSORS = [
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="ISO"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    JSONRenderer(sort_keys=True),
]


def get_wrapped_logger(logger_name: str = "root", **kwargs):
    """Returns a struct log equivalent for the named logger"""
    return structlog.wrap_logger(
        logging.getLogger(logger_name), COMMON_PROCESSORS, **kwargs
    )


class FormatJSONL(logging.Formatter):
    """Prevents printing of the stack trace to enable JSON lines output"""

    def formatException(self, ei):
        """Disables printing separate stack traces"""
        return


LOGGER = get_wrapped_logger("general")


class LogMainFunction:
    def __init__(self):
        self.logger = LOGGER

    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            try:
                result = fn(*args, **kwargs)
                return result
            except Exception as ex:
                self.logger.error(
                    "exception",
                    exception=ex.__str__(),
                    traceback=traceback.format_exc().splitlines(),
                )
                raise ex
            return result

        return decorated
