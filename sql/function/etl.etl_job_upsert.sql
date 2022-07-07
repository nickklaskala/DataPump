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
