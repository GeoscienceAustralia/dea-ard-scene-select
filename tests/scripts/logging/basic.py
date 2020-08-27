from dass_logs import LOGGER
from logging.config import fileConfig

fileConfig('basic_config.ini')

LOGGER.info('moo cow', state='ACT')


LOGGER.info('scene removed', datawet_id='bsrflkewjr', reason="scene id in ARD")