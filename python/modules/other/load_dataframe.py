import pandas as pd

def load_dataframe(etl_config):

	#standard variables
	logger       = etl_config['logger']
	env          = etl_config['env']
	datastore      = etl_config['datastore']
	file_path    = etl_config['file_path']
	logger.info('you are running load_dataframe()')

	#module specific variables
	config    = etl_config['config_ext'].get('load_dataframe_config',{})

	#dataframe specific variables
	dfName            =config.get('df_name','df')
	sep               =config.get('sep','|')
	dtype             =config.get('dtype','object')
	encoding          =config.get('encoding','utf8')

	#main
	logger.info('...loading file '+file_path)
	df = pd.read_csv(filepath_or_buffer=file_path,sep=sep,dtype=dtype,keep_default_na=False,encoding=encoding)
	etl_config[dfName]=df
	logger.info('...file loaded to generic dataframe')
