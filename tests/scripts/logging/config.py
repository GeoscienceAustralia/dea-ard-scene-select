import logs
from logging.config import fileConfig

fileConfig("logging.cfg")

# INTERFACE_LOGGER luigi-interface.log
# 2020-08-26 17:20:39,131: INFO: yeah

# STATUS_LOGGER status.log
# {"event": "zoo", "level": "info", "timestamp": "2020-08-26T07:23:04.788129Z"}

logs.INTERFACE_LOGGER.info("yeah")

logs.STATUS_LOGGER.info("zoo", state="yo")
# {"event": "zoo", "level": "info", "state": "yo", "timestamp": "2020-08-27T.."}

logs.TASK_LOGGER.info("flying", state="yo")
