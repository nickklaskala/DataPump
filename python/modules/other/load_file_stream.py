import io

#standard module to load file into in memory object
def load_file_stream(etl_config):
	'''  sample module config
	{"file_stream_name":null,"encoding":null}
	'''

	#standard variables
	logger         = etl_config['logger']
	file_path      = etl_config['file_path']

	logger.info('you are running load_file_stream()')

	#module specific variables
	config    = etl_config['config_ext'].get('load_file_stream_config',{})

	#dataframe specific variables
	file_stream_name  =config.get('file_stream_name','file_stream')
	encoding          =config.get('encoding','utf-8')

	#load file stream
	logger.info('...loading file '+file_path)
	f=open(file_path,'r',encoding=encoding).read()
	file_stream = io.StringIO(f)
	etl_config[file_stream_name]=file_stream
	logger.info('...file loaded to generic stream')

