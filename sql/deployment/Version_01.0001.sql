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


drop table if exists etl.etl_log;
create table etl.etl_log (
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
ALTER SEQUENCE etl.etl_log_log_id_seq RESTART WITH 10000;


drop table if exists etl.etl_datastore;
CREATE TABLE etl.etl_datastore (
	 datastore        text      not null primary key
	,create_date      timestamp not null default now()
	,update_date      timestamp not null default now()
);




drop function etl.etl_job_upsert;
create or replace function etl.etl_job_upsert(
	   v_job_name         text 
	,  v_is_active        boolean default null
	,  v_partner          text default null
	,  v_job_type         text default null
	,  v_source_loc       text default null
	,  v_destination_loc  text default null
	,  v_file_pattern     text default null
	,  v_entrypoint       text default null
	,  v_processes        text default null
	,  v_notes            text default null
	,  v_config_ext       json default null
	)
RETURNS integer
language plpgsql
as
$$
begin
	IF EXISTS (SELECT * FROM etl.etl_job WHERE job_name = v_job_name)
	then 
		update etl.etl_job set
			 job_name       =coalesce(v_job_name       ,etl_job.job_name       )
			,is_active      =coalesce(v_is_active      ,etl_job.is_active      )
			,partner        =coalesce(v_partner        ,etl_job.partner        )
			,job_type       =coalesce(v_job_type       ,etl_job.job_type       )
			,source_loc     =coalesce(v_source_loc     ,etl_job.source_loc     )
			,destination_loc=coalesce(v_destination_loc,etl_job.destination_loc)
			,file_pattern   =coalesce(v_file_pattern   ,etl_job.file_pattern   )
			,entrypoint     =coalesce(v_entrypoint     ,etl_job.entrypoint     )
			,processes      =coalesce(v_processes      ,etl_job.processes      )
			,notes          =coalesce(v_notes          ,etl_job.notes          )
			,config_ext     =coalesce(v_config_ext     ,etl_job.config_ext     )
		where job_name = v_job_name;
	else 
		INSERT INTO etl.etl_job (job_name,is_active,partner,job_type,source_loc,destination_loc,file_pattern,entrypoint,processes,notes,config_ext)
		VALUES(v_job_name,v_is_active,v_partner,v_job_type,v_source_loc,v_destination_loc,v_file_pattern,v_entrypoint,v_processes,v_notes,v_config_ext);
	end if;

	return 0;
end;
$$;


drop function etl.etl_log_init;
create or replace function etl.etl_log_init(in v_job_name text , in v_file_name text,in v_status text, out v_log_id text ) 
language plpgsql
as
$$
begin
	insert into etl.etl_log (job_name,file_name,status)\
	values 
		(v_job_name,v_file_name,v_status)
	RETURNING log_id into v_log_id;
end;
$$;


drop function etl.etl_log_update;
create or replace function etl.etl_log_update (
	   v_log_Id             int default null
	,  v_status             text default null
	,  v_log_level          text default null
	,  v_log_file           text default null
	,  v_file_row_count     int default null
	,  v_process_row_count  int default null
	,  v_error_row_count    int default null
	,  v_end_date           timestamp default null
	)
RETURNS integer
language plpgsql
as
$$
begin
	update etl.etl_log set
		 status           =coalesce(v_status           ,status)
		,log_level        =coalesce(v_log_level        ,log_level)
		,log_file         =coalesce(v_log_file         ,log_file)
		,file_row_count   =coalesce(v_file_row_count   ,file_row_count)
		,process_row_count=coalesce(v_process_row_count,process_row_count)
		,error_row_count  =coalesce(v_error_row_count  ,error_row_count)
		,end_date         =coalesce(v_end_date         ,end_date)
	where log_id=v_log_id;

	return 0;
end;
$$;






insert into etl.etl_datastore (datastore)
values ('CVS'),('WALLGREENS'),('DATAWAREHOUSE'),('OLTPSYSTEM'),('ETLSTAGING');


select etl.etl_job_upsert(
 v_job_name        =>'Sample_Job_Appointment_CVS'
,v_is_active       =>'t'
,v_datastore       =>'CVS'
,v_job_type        =>'inbound'
,v_source_loc      =>'//mynetworkdrive/CVS/{env}/incoming'
,v_destination_loc =>null
,v_file_pattern    =>'Appointment_CVS_*.csv'
,v_entrypoint      =>'inbound_single_file'
,v_processes       =>'load_file_stream,appointments'
,v_notes           =>null
,v_config_ext      =>'{"load_dataframe_config":{"dfName":"NicksDataFrame"}}'
);


select etl.etl_job_upsert(
 v_job_name        =>'Sample_Job_Fill_WALLGREENS'
,v_is_active       =>'t'
,v_datastore       =>'WALLGREENS'
,v_job_type        =>'inbound'
,v_source_loc      =>'//mynetworkdrive/_processing/Wallgreens/{env}/incoming'
,v_destination_loc =>null
,v_file_pattern    =>'Fill_WALLGREENS_*.csv'
,v_entrypoint      =>'inbound_single_file'
,v_processes       =>'load_dataframe,mask_phi,fill'
,v_notes           =>null
,v_config_ext      =>'{"appointment_config": {"DateStyle": "YYYYMMDD", "Headers": ["First_Name", "Last_Name"] }, "load_dataframe_config": {"dfName": "FillDataFrame", "sep": "|"} }'
);

