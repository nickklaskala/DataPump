drop table if exists shrx.etl_log;
create table shrx.etl_log (
	 log_id            serial
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
	,create_date       timestamp not null default now()
	,update_date       timestamp not null default now()
);
ALTER SEQUENCE shrx.etl_log_log_id_seq RESTART WITH 10000;