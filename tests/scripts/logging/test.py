
import structlog
from logs import COMMON_PROCESSORS, STATUS_LOGGER
from pythonjsonlogger import jsonlogger

structlog.configure(processors=COMMON_PROCESSORS)
_LOG = structlog.get_logger("insar")

log_pathname = 'yo.txt'

# I don't want to do this...
with open(log_pathname, "w") as fobj:
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(fobj))

    _LOG.info("processing _get_required_grids") #, pathname=vector_file)
STATUS_LOGGER.info("go")