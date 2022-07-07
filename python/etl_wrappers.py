'''
Standard ETL wrapper to control launching linking and logging etl procedures into one homogeneous job

Prerequisistes
	Environment variables

		to edit your environment open powershell and type :  rundll32 sysdm.cpl,EditEnvironmentVariables
		you can also set up overrides in pycharm in the debug configurationbs but i advise you do this step once so you dont have to edit pycharm often.

		ENV: dev/prod/test. you will edit your local environment variables to dev or test and the respective server will always have either dev/prod/test in the ENV environment variable
		PROCESSINGDIR: path to ~processing. I suggest for dev\testing you create your own local structure example:c:\repos\datapump\processing. can also use the network drive
		PGUSER: user running the etl that will be used for postgres database authentication
		PGPASSWORD: password of user running the etl that will be used for postgres database authentication

		samples
			ENV:dev
			PROCESSINGDIR:c:/repos/datapump/_processing  or K:/_processing  (this needs to be your local directory in dev and production location on servers)
			PGUSER:nklaskala
			PGPASSWORD:gtfoh

	Configuration
		all configuration comes a unique record  in staginginfo that specifies EVERYTHING about how this job will run
		running elt_main_entry will always ONLY call itself with 1 argument and that argument is the name of the staginginfo.name in which you want to run

	variable
		there are 3 types of variables

		global variables: env/rootdir is set up on the job level by the wrapper, then get dumped into etlConfig dictionary
		module specific variables: json table values in etlConfig.config_ext
		etl variables: direct fields from etl_job table dumped into etlConfig dictionary


'''

#standard library
import os
import argparse
import glob
import shutil

#shs
import datapump_utils
import modules



def inbound_single_file(job_name,debug=False):

	#Environment
	env                          = os.environ['ENV']
	processing_dir               = os.environ['PROCESSINGDIR']
	datapump_dir                 = processing_dir+'/datapump/'+env
	maestro                      = datapump_utils.Maestro(env)

	#load configurations for etl job
	etl_config                   = maestro.lookup_etl_config(job_name=job_name)

	#get external file
	external_file_source_path    = (glob.glob(etl_config['source_loc']+'/'+etl_config['file_pattern']))[0]
	external_file_name           = os.path.basename(external_file_source_path)
	external_file_archive_path   = etl_config['source_loc']+'/archive/'+external_file_name

	#generate unique log number for this run and setup logger
	me                           = maestro.is_single_instance(flavor=etl_config['job_id'])#immediately kill me  if another one is running with same job_name.
	maestro.etl_log_reset(job_name=job_name)#reset incomplete runs
	log_id                       = maestro.etl_log_init(job_name=job_name,file_name=external_file_name,status='running')
	logger                       = datapump_utils.MyLogger(log_dir=datapump_dir+'/logging',base_name=job_name,log_id=log_id,maestro=maestro)

	#get internal locations
	internal_staged_path         = datapump_dir+'/jobData/'+job_name+'/staged/'
	internal_processed_path      = datapump_dir+'/jobData/'+job_name+'/processed/'
	internal_errored_path        = datapump_dir+'/jobData/'+job_name+'/errored/'
	internal_file_staged_path    = internal_staged_path+external_file_name
	internal_file_processed_path = internal_processed_path+external_file_name
	internal_file_errored_path   = internal_errored_path+external_file_name

	#maintain internal directory structure
	os.makedirs(internal_staged_path, exist_ok=True)
	os.makedirs(internal_processed_path, exist_ok=True)
	os.makedirs(internal_errored_path, exist_ok=True)
	shutil.copy(external_file_source_path,internal_staged_path)
	internal_file_stage_path=internal_staged_path+'/'+external_file_name

	#archive external file
	if not debug:
		shutil.move(external_file_source_path,external_file_archive_path)

	#pass to shelf
	etl_config['file_name']         = external_file_name
	etl_config['file_path']         = internal_file_staged_path
	etl_config['logger']            = logger
	etl_config['env']               = env
	etl_config['processing_dir']    = processing_dir
	etl_config['datapump_dir']      = datapump_dir
	etl_config['maestro']           = maestro
	etl_config['datapump_utils']    = datapump_utils

	#process
	for step in etl_config['processes'].split(','):
		process = getattr(modules, step)
		try:
			process(etl_config)
		except Exception as err:
			logger.error(str(err))
			maestro.etl_log_update(log_id=log_id,status='done',end_date=True)
			shutil.move(internal_file_stage_path,internal_file_errored_path)
			raise

	#archive file internally
	shutil.move(internal_file_stage_path,internal_file_processed_path)

	#el fin
	maestro.etl_log_update(log_id=log_id,status='done',end_date=True)
	logger.close()


if __name__=='__main__':
	#set up parser
	argparser           =  argparse.ArgumentParser()
	argparser.add_argument("-j",  "--job_name" ,help="",required=True)
	argparser.add_argument("-b",  "--debug" ,help="",required=False,default='False')

	#parse args
	args                =  argparser.parse_args()
	job_name            =  args.job_name
	debug               =  bool(args.debug.lower()=='true')

	#determine etl wrapper
	maestro             = datapump_utils.Maestro(os.environ['ENV'])
	etl_config          = maestro.lookup_etl_config(job_name=job_name)

	#launch etl
	if etl_config['entrypoint']=='inbound_single_file':
		inbound_single_file(job_name=job_name,debug=debug)