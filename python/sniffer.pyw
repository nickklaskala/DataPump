#!/usr/bin/python3

#standard lib
import os 
import time
import glob
import threading

#shs
import datapump_utils
import datapump_etl_entry

#set up environment
env            = os.environ['ENV']
processing_dir = os.environ['PROCESSINGDIR']
db             = datapump_utils.Datastore.easy_connect(env=env,datastore='ETLSTAGING',autocommit=True)


loop=0
while True:

	loop+=1
	#get triggered jobs to process
	jobs = db.execute('select * from etl.etl_job where is_active=$$t$$ and job_type=$$tiggered-file-drop$$ and source_loc is not null and file_pattern is not null',return_dict=True)

	# look for files
	for job in jobs:
		source_loc   = job['source_loc'].replace('{env}',env)
		file_pattern = job['file_pattern']
		print('searching for '+source_loc+'/'+file_pattern)
		file_name    = glob.glob(source_loc+'/'+file_pattern)
		print(file_name)

		#launch job
		if file_name:
			process=getattr(datapump_etl_entry,job['entrypoint'])
			print('launching job {job_name}'.format(job_name=job['job_name']))
			thread=threading.Thread(target=process,args=[job['job_name']])
			thread.start()




	#get scheduled jobs to process
	db.execute('select etl.etl_forecast_build()')
	jobs = db.execute('select * from etl.etl_forecast as a left join etl.etl_job as b on b.job_id=a.job_id where a.is_started=false and start_datetime<now()',return_dict=True)
	jobs = [jobs] if isinstance(jobs, dict) else jobs

	print()
	#launch job
	for job in jobs:
		etl_forecast_id=job['etl_forecast_id']
		process=getattr(datapump_etl_entry,job['entrypoint'])
		print('launching job {job_name}'.format(job_name=job['job_name']))
		thread=threading.Thread(target=process,args=[job['job_name'],etl_forecast_id])
		thread.start()
		time.sleep(2)


	#go to sleep
	time.sleep(5)

	#dev testing
	if loop==3  or glob.glob(processing_dir+'/datapump/'+env+'/maestro/datapump_stop*'):
		break


