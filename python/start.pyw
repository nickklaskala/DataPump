#!/usr/bin/python3

#standard lib
import os 
import time
import glob
import threading

#shs
import datapump_utils
import etl_wrappers

#set up environment
env            = os.environ['ENV']
processing_dir = os.environ['PROCESSINGDIR']
db             = datapump_utils.Datastore.easy_connect(env=env,datastore='etl',autocommit=True)

while True:
	#get jobs to process
	jobs = db.execute('select * from etl.etl_job where is_active=$$t$$ and job_type=$$inbound$$ and source_loc is not null and file_pattern is not null',return_dict=True)

	# look for files
	for job in jobs:
		source_loc   = job['source_loc'].replace('{env}',env)
		file_pattern = job['file_pattern']
		print('searching for '+source_loc+'/'+file_pattern)
		file_name    = glob.glob(source_loc+'/'+file_pattern)
		print(file_name)

		#launch job
		if file_name:
			with open(processing_dir+'/datapump/'+env+'/maestro/sniffer.log', 'a') as f:
				print('launching job {job_name}'.format(job_name=job['job_name']), file=f)
			print('launching job {job_name}'.format(job_name=job['job_name']))
			process=getattr(datapump_etl_wrappers,job['entrypoint'])
			thread=threading.Thread(target=process,args=[job['job_name']])
			thread.start()

	#go to sleep
	time.sleep(5)


	#adding ability to force kill the process by dropping a stop file in a certain location. ideally you would remove this before migrating to prod
	if glob.glob(processing_dir+'/datapump/'+env+'/maestro/datapump_stop*'):
		break



