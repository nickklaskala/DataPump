# DataPump

## Overview
- DataPump is a quick and painless python etl framework that provides the essential basic tools for building ETL from configuration, logging, file flow, scheduling (push/file drop, pull/scheduled), and monitoring.
- It is relatively barebones so you can add and extend it where you see fit. Anything can be changed and is recommended to change some things.
- The start.pyw is a starting point to sniff out inbound etl routines, you can further add outbound scheduling and whatever else you like just make sure the only functionality is starting a thread(etl job).
- The wrappers are just canned "shapes" of how a etl typically flows. It collects the assets and stores are the variable information about the etl job. It "sets the stage" for the etl while the Modules do the actual etl. feel free to add wrappers for inbound/outbound/maintenance jobs as well as multi file jobs.
- *Modules* is just sample routines. Please build your etls here. There is some canned mechinisms for loading csvs to filestreams and dataframes and a personal contribution for masking sensitive information *mask_phi*
- The essential aspect of datapump is the *etl_config* python dictionary that gets passed to each *Module* within the job. *etl_config* operates like a shelf.  You add things to it and take things from it and then the next module comes along and adds to it and takes from it. the process list in the configuration table is just the sequence of modules that will get passed the shelf until the job is done.


## How to get up and running
1. Check out repo
2. Set up environment variables (ENV,PROCESSINGDIR,USER,PASSWORD)and add ~DataPump/Python to your python path
3. Remove dummy datastores in python/datapump_utils. Edit your datastore list to include your datastores engine,host,port,and database.
	 You can also import this info however you like just need a dictionary at the end of the day in utils.
4. If you dont use a postgres engine you will need to extend the Datastore class in datapump_utils to exhibit behavior like the Datastore class or create your own class
5. Deploy sql - Create etl schema or whatever you decide to name it and deploy database tables to hold etl configs and logging tables
6. Configure a job with etl.etl_job_upsert
7. To run the job locally you can run the etl_wrapper.py in your ide with the sole parameter as the job_name from your configuration table of the job you wish to run/debug. To automate running the jobs on the server start the start.pyw on the server.


## Notes
- everythong is operating system and environment agnostic.You should be able to run the jobs from your local and from your dev/test/prod servers(linux/mac/windows) all the same.
- everything is flexible/extensible. The core of datapump is the classes datapump_utils.  start.pyw and etl_wrappers are just a starting point for how to "shape" your etl. the modules are just different "phases" of the etl. the sql is just the configuration store and the logging/visibilty of the system.

## Help
- create an issue on this github page and I'll gladly set up some time to help you get set up.

