
from .appointments_utils import ap_fun

def appointments(etl_config):
	logger=etl_config['logger']
	logger.info('you are runnning appointments()')
	ap_fun(etl_config)

