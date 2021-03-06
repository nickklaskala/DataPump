drop table if exists shrx.etl_job;
create TABLE shrx.etl_job (
	 job_id           serial    not null
	,job_name         text      not null UNIQUE
	,is_active        boolean   not null default 't'
	,partner          text      not null 
	,source_loc       text          null
	,destination_loc  text          null
	,job_type         text      not null CHECK (job_type in('scheduled','tiggered-file-drop','manual'))
	,date_signature   text          null
	,file_pattern     text          null
	,entrypoint       text      not null
	,processes        text      not null default ''
	,notes            text          null
	,sumologic        text          null
	,config_ext       json          null
	,create_date      timestamp not null default now()
	,update_date      timestamp not null default now()
);
ALTER SEQUENCE shrx.etl_job_job_id_seq RESTART WITH 10000;

COMMENT ON COLUMN shrx.etl_job.date_signature IS 'first seven characters at bit representation of monday-sunday the commadelimited 4 digit numbers are the times of the day';


