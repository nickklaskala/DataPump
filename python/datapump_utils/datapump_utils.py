import os
import logging
from datetime import datetime
import psycopg2
import psycopg2.extras
import sqlalchemy

class Maestro:
	"""class that stores a connection to the etl database and performs special etl actions

	Attributes:
		db_etl the common db location for storing information about the etl job

	Actions
		lookup_etl_config:takes the job_name and self attribute environment and retrieves all information about the job to run from etl
		etl_log_upsert:takes job_name, file_name, and status and Creates a log intry in etl.etl_log, and returns a log_id which is associated to this execution of the etl/job
		etl_log_update:updates the before mentioned log record associated to the run with meta about the run (rows,status,log level,start and end times)
	"""

	def __init__(self,env):
		self.env=env
		self.db =Datastore.easy_connect(env,'ETLSTAGING',autocommit =True)

	def lookup_etl_config(self,job_name):
		sql="select * from etl.etl_job where job_name='{0}'".format(job_name)
		result=self.db.execute(sql=sql,return_dict=True)[0]
		result['source_loc']=(result['source_loc'] or '').replace('{env}',self.env)
		result['destination_loc']=(result['destination_loc'] or '').replace('{env}',self.env)
		return result

	def etl_log_upsert(self,log_id='NULL',job_id=None,file_name=None,status=None,log_level=None,log_file=None,file_row_count=None,process_row_count=None,error_row_count=None,end_date=None):
		sql="v_log_id=>{}".format(log_id)
		if job_id:
			sql+=",v_job_id=>'{}'".format(job_id)
		if file_name:
			sql+=",v_file_name=>'{}'".format(file_name)
		if status:
			sql+=",v_status=>'{}'".format(status)#running/complete/error/reset
		if log_level:
			sql+=",v_log_level=>'{}'".format(log_level)
		if log_file:
			sql+=",v_log_file=>'{}'".format(log_file)
		if file_row_count:
			sql+=",v_file_row_count=>'{}'".format(file_row_count)
		if process_row_count:
			sql+=",v_process_row_count=>'{}'".format(process_row_count)
		if error_row_count:
			sql+=",v_error_row_count=>'{}'".format(error_row_count)
		if end_date:
			sql+=",v_end_date=>now()::timestamp"
		sql="select etl.etl_log_upsert({0})".format(sql)
		result=self.db.execute(sql=sql,return_dict=True)[0]
		return str(result['etl_log_upsert'])

	def etl_log_reset(self,job_id):
		sql="update etl.etl_log set status='reset' where job_id='{0}' and status='running'".format(job_id)
		result=self.db.execute(sql=sql,return_dict=True)
		return result

	@staticmethod
	def is_single_instance(flavor):
		# this will throw an error if same port is currently in use.  We can control concurrency by initializing this with each job run if that job should not be ran in parrallel
		# this essentially binds a job to a port and if the port is already bound it will throw an error
		# assumptions port 60000-65000 not in use by other programs
		# can use any method we want if this becomes a problem. sockets is a standard approach.  as long as this function fails if concurrent
		import socket
		instance = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		port=flavor+60000
		try:
			instance.bind(('localhost', port))
		except:
			raise Exception('job_id '+str(flavor)+' already running and does not allow concurrency.  Will try again on next loop/or next scheduled time')
		return instance


class Datastore:
	"""composite class for interfacing with datastores

		notes:
			the composite classes (engines) should all support the same behavior is query/commit
			currently only supports postgres but is extensible to other engines through class composition

	"""

	def __init__(self,engine,host,port,database,user,password,autocommit=True):
		if engine=='postgres':
			self.db=self.Postgres(host,port,database,user,password,autocommit)
		elif engine=='sqlserver':
			pass

		self.connection=self.db.connection
		self.execute=self.db.execute
		self.commit=self.db.commit
		self.cursor=self.db.cursor

	@staticmethod
	def easy_connect(env,datastore,autocommit=True,user=None,password=None):
		my_db=Datastore(engine    =datastores[env][datastore]['engine']
					  ,host      =datastores[env][datastore]['host']
					  ,port      =datastores[env][datastore]['port']
					  ,database  =datastores[env][datastore]['database']
					  ,user      =user       or os.environ['PGUSER']
					  ,password  =password   or os.environ['PGPASSWORD']
					  ,autocommit=autocommit)
		return my_db

	@staticmethod
	def easy_connect_sqlalchemy(env,datastore,user=None,password=None):
		host      =datastores[env][datastore]['host']
		port      =datastores[env][datastore]['port']
		database  =datastores[env][datastore]['database']
		user      =user       or os.environ['PGUSER']
		password  =password   or os.environ['PGPASSWORD']
		string='postgresql://{user}:{password}@{host},{port}/{database}'.format(user=user,password=password,host=host,port=port,database=database)
		sqlalchemy_engine=sqlalchemy.create_engine(string)
		return sqlalchemy_engine

	@staticmethod
	def easy_connect_psycopg2(env,datastore,user=None,password=None):
		host      =datastores[env][datastore]['host']
		port      =datastores[env][datastore]['port']
		database  =datastores[env][datastore]['database']
		user      =user       or os.environ['PGUSER']
		password  =password   or os.environ['PGPASSWORD']
		connection=psycopg2.connect(host=host,port=port,database=database,user=user,password=password)
		return connection

	class Postgres:
		def __init__(self,host,port,database,user,password,autocommit):
			self.connection=self.connect(host,port,database,user,password)
			self.cursor    =self.connection.cursor()
			self.cursor_dict=self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
			if autocommit:
				self.connection.autocommit=True

		def connect(self,host,port,database,user,password):
			connection=psycopg2.connect(host=host,port=port,database=database,user=user,password=password)
			return connection

		def execute(self,sql,return_dict=False):
			if return_dict:
				self.cursor_dict.execute(sql)
				try:
					result=self.cursor_dict.fetchall()
					try:
						# print(result)
						result=[dict(row) for row in result] #if RealDictRow is returned try formatting it
					except:
						pass
					# return result[0] if len(result)==1 else result # should this be consistent across all return types?
					return result # should this be consistent across all return types?
				except:
					pass
			else:
				self.cursor.execute(sql)
				try:
					result=self.cursor.fetchall()
					return result
				except:
					pass

		def commit(self):
			self.connection.commit()


class MyLogger:
	""" wrapper class for pythons logging module

		notes: sets up 4 logging objects current/info/error/warning
			each object writes to a different location
			the archive log is the true log and archives itself into respective year/month/day/folders
			the other 3 logs are deleteable/informative and only there for increased visibility to the organization
	"""

	def __init__(self,log_dir,base_name,log_id,maestro):
		#get attributes
		self.log_dir          =log_dir
		self.base_name        =base_name
		self.log_id           =log_id
		self.highest_seen_level=''
		self.maestro         =maestro

		#maintain log directory
		self.build_archive(log_dir=log_dir)

		#build Log objects for run
		_Y,_m,_d,_H,_M,_S = datetime.now().strftime("%Y,%m,%d,%H,%M,%S").split(',')
		path_info=log_dir+'/info/'+base_name+'_info.log'
		path_error=log_dir+'/error/'  +base_name+'_error.log'
		path_warning=log_dir+'/warning/'+base_name+'_warning.log'
		path_archive=log_dir+'/archive/'+_Y+'/'+_m+'/'+_d+'/'+base_name+'_archive_'+_Y+_m+_d+_H+_M+_S+'_'+log_id+'.log'

		#setup
		self.setup_logger(name='log_info'+str(log_id)   ,log_id=log_id,mode='a' ,stream=True ,path=path_info)
		self.setup_logger(name='log_error'+str(log_id)  ,log_id=log_id,mode='a'              ,path=path_error)
		self.setup_logger(name='log_warning'+str(log_id),log_id=log_id,mode='a'              ,path=path_warning)
		self.setup_logger(name='log_archive'+str(log_id),log_id=log_id,mode='w'              ,path=path_archive)

		#get logger objects
		self.log_info    = logging.getLogger('log_info'+str(log_id))
		self.log_error   = logging.getLogger('log_error'+str(log_id))
		self.log_warning = logging.getLogger('log_warning'+str(log_id))
		self.log_archive = logging.getLogger('log_archive'+str(log_id))

		maestro.etl_log_upsert(log_id=log_id,log_file=path_archive)

	def setup_logger(self, name, path, log_id, stream=False,mode='w'):
		log = logging.getLogger(name)
		formatter = logging.Formatter('%(asctime)s.%(msecs)03d - {log_id:>10s} - %(levelname)-10s - %(message)s'.format(log_id=log_id), datefmt='%Y-%m-%d %H:%M:%S')
		fileHandler = logging.FileHandler(path, mode=mode)
		fileHandler.setFormatter(formatter)
		log.setLevel(logging.INFO)
		log.addHandler(fileHandler)

		if stream:
			stream_handler = logging.StreamHandler()
			stream_handler.setFormatter(formatter)
			log.addHandler(stream_handler)

	def close(self):
		for log in (self.log_info, self.log_error, self.log_warning, self.log_archive):
			for handler in log.handlers:
				if isinstance(handler, logging.FileHandler):
					handler.close()

	def build_archive(self,log_dir):
		_Y,_m,_d = datetime.now().strftime("%Y,%m,%d").split(',')
		path_to_make1=log_dir+'/archive/'+_Y+'/'+_m+'/'+_d
		path_to_make2=log_dir+'/info'
		path_to_make3=log_dir+'/warning'
		path_to_make4=log_dir+'/error'
		os.makedirs(path_to_make1, exist_ok=True)
		os.makedirs(path_to_make2, exist_ok=True)
		os.makedirs(path_to_make3, exist_ok=True)
		os.makedirs(path_to_make4, exist_ok=True)

	def info(self,msg):
		self.set_status('info')
		for line in msg.split('\n'):
			self.log_info.info(line)
			self.log_archive.info(line)

	def warning(self,msg):
		self.set_status('warning')
		for line in msg.split('\n'):
			self.log_info.warning(line)
			self.log_archive.warning(line)
			self.log_warning.warning(line)

	def error(self,msg):
		self.set_status('error')
		for line in msg.split('\n'):
			self.log_info.error(line)
			self.log_archive.error(line)
			self.log_error.error(line)
			self.log_warning.error(line)

	def set_status(self, level):
		levels={'info':1,'warning':2,'error':3}
		current_level=levels.get(self.highest_seen_level,0)
		new_level=levels.get(level)
		if new_level>current_level:
			self.highest_seen_level=level
			self.maestro.etl_log_upsert(self.log_id,log_level=level)


datastores= {
	'dev':{
		 'DATAWAREHOUSE'  : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'enterpriseDWdev'  }
		,'OLTPSYSTEM'     : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'deoltp'           }
		,'ETLSTAGING'     : {'engine':'postgres','host': 'localhost', 'port': '5432', 'database': 'postgres'         }
		,'CVS'            : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'dev_cvs'          }
		,'WALLGREENS'     : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'wallgreens_test'  }
	}
	,'local':{
		 'DATAWAREHOUSE'  : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'enterpriseDWtest' }
		,'OLTPSYSTEM'     : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'oltptest'         }
		,'ETLSTAGING'     : {'engine':'postgres','host': 'localhost', 'port': '5432', 'database': 'postgres'         }
		,'CVS'            : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'cvs_test'         }
		,'WALLGREENS'     : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'wallgreens_test'  }
	}
	,'prod':{
		 'DATAWAREHOUSE'  : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'enterpriseDW'     }
		,'OLTPSYSTEM'     : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'enterpriseoltp'   }
		,'ETLSTAGING'     : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'etl_prod'         }
		,'CVS'            : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'cvs_prod'         }
		,'WALLGREENS'     : {'engine':'postgres','host': 'localhost', 'port': '5453', 'database': 'wallgreens'       }
	}
}

 


#test
if __name__=='__main__':
	for env,datastores_dict in datastores.items():
		for datastore,connection in datastores_dict.items():
			try:
				db=Datastore.easy_connect(env=env,datastore=datastore)
				rst=db.execute('select current_database()')
				print(f'{env}.{datastore}---{rst}')
			except:
				print(f'****could not connect to {env}.{datastore}')








