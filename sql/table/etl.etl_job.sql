drop table if exists etl.etl_job;
create TABLE etl.etl_job (
	 job_id           serial    not null
	,job_name         text      not null UNIQUE
	,is_active        boolean   not null default 't'
	,partner          text      not null references etl.etl_partner (partner)
	,source_loc       text          null
	,destination_loc  text          null
	,job_type         text      not null CHECK (job_type in('inbound','outbound','maintenance'))
	,file_pattern     text          null
	,entrypoint       text      not null
	,processes        text      not null default ''
	,notes            text          null
	,sumologic        text          null
	,config_ext       json          null
	,create_date      timestamp not null default now()
	,update_date      timestamp not null default now()
);
ALTER SEQUENCE etl.etl_job_job_id_seq RESTART WITH 10000;


