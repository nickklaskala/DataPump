import io

def load_file_stream(etl_config):
	logger         = etl_config['logger']
	file_path      = etl_config['file_path']

	logger.info('you are running load_file_stream()')

	#module specific variables
	config    = etl_config['config_ext'].get('load_file_stream_config',{})

	#dataframe specific variables
	file_stream_name  =config.get('file_stream_name','file_stream')


	logger.info('...loading file '+file_path)
	f=open(file_path,'r').read()
	file_stream = io.StringIO(f)
	etl_config[file_stream_name]=file_stream
	logger.info('...file loaded to generic stream')
