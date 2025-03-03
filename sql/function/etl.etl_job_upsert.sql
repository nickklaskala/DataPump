drop function if exists etl.etl_job_upsert;
create or replace function etl.etl_job_upsert(
	   v_job_name         text 
	,  v_is_active        boolean
	,  v_datastore        text default 'xNULLx'
	,  v_job_type         text default 'xNULLx'
	,  v_date_signature   text default 'xNULLx'
	,  v_source_loc       text default 'xNULLx'
	,  v_destination_loc  text default 'xNULLx'
	,  v_file_pattern     text default 'xNULLx'
	,  v_entrypoint       text default 'xNULLx'
	,  v_processes        text default 'xNULLx'
	,  v_notes            text default 'xNULLx'
	,  v_config_ext       json default '{"xNULLx":"xNULLx"}'
	)
RETURNS integer
language plpgsql
as
$$
begin
	IF EXISTS (SELECT * FROM etl.etl_job WHERE job_name = v_job_name)
	then 
		update etl.etl_job set
			 job_type       = case v_job_type         when 'xNULLx'              then etl_job.job_type        else v_job_type        end
			,date_signature = case v_date_signature   when 'xNULLx'              then etl_job.date_signature  else v_date_signature  end
			,is_active      = case v_is_active        when null                  then etl_job.is_active       else v_is_active       end
			,entrypoint     = case v_entrypoint       when 'xNULLx'              then etl_job.entrypoint      else v_entrypoint      end
			,file_pattern   = case v_file_pattern     when 'xNULLx'              then etl_job.file_pattern    else v_file_pattern    end
			,source_loc     = case v_source_loc       when 'xNULLx'              then etl_job.source_loc      else v_source_loc      end
			,destination_loc= case v_destination_loc  when 'xNULLx'              then etl_job.destination_loc else v_destination_loc end
			,datastore      = case v_datastore        when 'xNULLx'              then etl_job.datastore       else v_datastore       end
			,processes      = case v_processes        when 'xNULLx'              then etl_job.processes       else v_processes       end
			,config_ext     = case v_config_ext::text when '{"xNULLx":"xNULLx"}' then etl_job.config_ext      else v_config_ext      end
			,notes          = case v_notes            when 'xNULLx'              then etl_job.notes           else v_notes           end
		where job_name = v_job_name;
	else 
		INSERT INTO etl.etl_job (job_name,is_active,datastore,job_type,date_signature,source_loc,destination_loc,file_pattern,entrypoint,processes,notes,config_ext)
		VALUES(v_job_name,v_is_active,v_datastore,v_job_type,v_date_signature,v_source_loc,v_destination_loc,v_file_pattern,v_entrypoint,v_processes,v_notes,v_config_ext);
	end if;

	return 0;
end;
$$;

