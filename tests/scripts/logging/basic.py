from dass_logs import LOGGER
from logging.config import fileConfig
from pathlib import Path

the_logfilename = Path(__file__).parent.joinpath("basic.log").resolve()
print (the_logfilename)
fileConfig('basic_config.ini' , disable_existing_loggers=False, defaults={ 'logfilename' :  '/g/data/u46/users/dsg547/sandpit/dea-ard-scene-select/tests/scripts/logging/basic.log'} )
#fileConfig('basic_config.ini' , disable_existing_loggers=False, defaults={ 'logfilename' :  str(the_logfilename)} )
#fileConfig('basic_config.ini' , disable_existing_loggers=False, defaults={ 'logfilename' :  '/g/data/u46/users/dsg547/sandpit/dea-ard-scene-select/tests/scripts/logging/scratch/logals.txt'} )

LOGGER.info('moo cow', state='ACT')


LOGGER.info('scene removed', datawet_id='bsrflkewjr', reason="scene id in ARD")
