import logs
from logging.config import fileConfig

fileConfig('logging.cfg')

logs.INTERFACE_LOGGER.info('yeah')
logs.STATUS_LOGGER.info('zoo')
logs.TASK_LOGGER.info('flying')