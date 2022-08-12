import pandas as pd

#standard module to simply load file data to unadulterated dataframe(all dtype objects)
def load_dataframe(etl_config):
	''' sample config for module
	{"df_name"=null,"sep"=null,"dtype"=null,"encoding"=null}
	'''

	#standard variables
	logger       = etl_config['logger']
	file_path    = etl_config['file_path']
	logger.info('you are running load_dataframe()')

	#module specific variables
	config    = etl_config['config_ext'].get('load_dataframe_config',{})

	#dataframe specific variables
	df_name           =config.get('df_name','df')
	sep               =config.get('sep','|')
	dtype             =config.get('dtype','object')
	encoding          =config.get('encoding','utf8')

	#main
	logger.info('...loading file '+file_path)
	df = pd.read_csv(filepath_or_buffer=file_path,sep=sep,dtype=dtype,keep_default_na=False,encoding=encoding)
	df = df.applymap(lambda x: x.strip() if isinstance(x, object) else x)
	etl_config[df_name]=df
	logger.info('...file loaded to generic dataframe')

