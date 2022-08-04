drop table if exists etl.etl_log;
create table etl.etl_log (
	 log_id            serial
	,job_id            int REFERENCES etl.etl_job (job_id) not null
	,job_name          text not null
	,file_name         text
	,status            text CHECK (status in('running','done','reset'))
	,log_level         text CHECK (status in('info','error','warning') or status=null)
	,log_file          text
	,file_row_count    int
	,process_row_count int
	,error_row_count   int
	,start_date        timestamp not null default now()
	,end_date          timestamp null 
);
