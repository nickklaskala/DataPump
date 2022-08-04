drop function if exists etl.etl_job_upsert;
create or replace function etl.etl_job_upsert(
	   v_job_name         text 
	,  v_is_active        boolean default null
	,  v_partner          text default 'xNULLx'
	,  v_job_type         text default 'xNULLx'
	,  v_source_loc       text default 'xNULLx'
	,  v_destination_loc  text default 'xNULLx'
	,  v_file_pattern     text default 'xNULLx'
	,  v_entrypoint       text default 'xNULLx'
	,  v_processes        text default 'xNULLx'
	,  v_notes            text default 'xNULLx'
	,  v_sumologic        text default 'xNULLx'
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
			 is_active      = coalesce(v_is_active,etl_job.is_active)
			,partner        = case v_partner          when 'xNULLx'              then etl_job.partner         else v_partner         end
			,job_type       = case v_job_type         when 'xNULLx'              then etl_job.job_type        else v_job_type        end
			,source_loc     = case v_source_loc       when 'xNULLx'              then etl_job.source_loc      else v_source_loc      end
			,destination_loc= case v_destination_loc  when 'xNULLx'              then etl_job.destination_loc else v_destination_loc end
			,file_pattern   = case v_file_pattern     when 'xNULLx'              then etl_job.file_pattern    else v_file_pattern    end
			,entrypoint     = case v_entrypoint       when 'xNULLx'              then etl_job.entrypoint      else v_entrypoint      end
			,processes      = case v_processes        when 'xNULLx'              then etl_job.processes       else v_processes       end
			,notes          = case v_notes            when 'xNULLx'              then etl_job.notes           else v_notes           end
			,sumologic      = case v_sumologic        when 'xNULLx'              then etl_job.sumologic       else v_sumologic       end
			,config_ext     = case v_config_ext::text when '{"xNULLx":"xNULLx"}' then etl_job.config_ext      else v_config_ext      end
		where job_name = v_job_name;
	else 
		INSERT INTO etl.etl_job (job_name,is_active,partner,job_type,source_loc,destination_loc,file_pattern,entrypoint,processes,notes,sumologic,config_ext)
		VALUES(v_job_name,v_is_active,v_partner,v_job_type,v_source_loc,v_destination_loc,v_file_pattern,v_entrypoint,v_processes,v_notes,v_sumologic,v_config_ext);
	end if;

	return 0;
end;
$$;
