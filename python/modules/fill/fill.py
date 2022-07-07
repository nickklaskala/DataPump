import datapump_utils
from .fill_utils.custom_transforms import custom_transforms

def fill(etl_config):
	cfg=etl_config['config_ext']['fill_config']
	inFile=etl_config['file_path']
	bshDate=datetime.now().strftime("%Y%m%d")

	logger=etl_config['logger']
	env=etl_config['env']
	datastore=etl_config['datastore']

	for step in cfg.get('custom_transforms',[]):
		process = getattr(custom_transforms,step)
		try:
			process(etl_config=etl_config)
		except Exception as err:
			logger.error(str(err))
			raise

	logger.info('performing fill etl')


