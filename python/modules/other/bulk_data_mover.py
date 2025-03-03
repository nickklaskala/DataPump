import pandas as pd
import datapump_utils as sfu

def bulk_data_mover(etl_config):
	'''
	#simple module or moving tables/data between environments/databases
	#sample config for module
		{
		 "if_exists"=null
		,"src_env"=null
		,"src_datastore"=null
		,"src_schema"=null
		,"src_table_name"=null
		,"trg_env"=null
		,"trg_datastore"=null
		,"trg_schema"=null
		,"trg_table_name"=null
		}
	'''

	#standard variables
	logger       = etl_config['logger']
	logger.info('you are running bulk_data_mover()')

	#module specific variables
	config         =etl_config['config_ext']['bulk_data_mover_config']
	if_exists      =config.get('if_exists','append')
	src_env        =config['src_env']
	src_datastore  =config['src_datastore']
	src_schema     =config['src_schema']
	src_table_name =config['src_table_name']
	trg_env        =config['trg_env']
	trg_datastore  =config['trg_datastore']
	trg_schema     =config['trg_schema']
	trg_table_name =config['trg_table_name']

	#main
	src_db=sfu.Database.easy_connect_sqlalchemy(src_env,src_datastore)
	trg_db=sfu.Database.easy_connect_sqlalchemy(trg_env,trg_datastore)

	df=pd.read_sql_table(schema=src_schema,table_name=src_table_name, con=src_db)
	df.to_sql(name=trg_table_name, schema=trg_schema, con=trg_db ,if_exists=if_exists, index=False,chunksize=1000)


if __name__=='__main__':

	pass
	# db1=sfu.Database.easy_connect_sqlalchemy('prod','WALMART')
	# db2=sfu.Database.easy_connect_sqlalchemy('test','WALMART')

	# df=pd.read_sql_table(schema='sales',table_name='sales_volume', con=db1)
	# df.to_sql(name='sales_volume_backup2', schema='sales', con=db2 ,if_exists='append', index=False,chunksize=2000)
