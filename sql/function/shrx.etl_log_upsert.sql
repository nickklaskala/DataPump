drop function if exists shrx.etl_log_upsert;
create or replace function shrx.etl_log_upsert (
	 v_log_Id             int default null
	,v_job_id             int default null
	,v_file_name          text default null
	,v_status             text default null
	,v_log_level          text default null
	,v_log_file           text default null
	,v_file_row_count     int default null
	,v_process_row_count  int default null
	,v_error_row_count    int default null
	,v_end_date           timestamp default null
	)
RETURNS integer
language plpgsql
as
$$
begin
	IF EXISTS (SELECT * FROM shrx.etl_log WHERE log_id = v_log_Id)
	then

		update shrx.etl_log set
			 log_Id           =coalesce(v_log_Id            ,etl_log.log_Id            )
			,job_id           =coalesce(v_job_id            ,etl_log.job_id            )
			,file_name        =coalesce(v_file_name         ,etl_log.file_name         )
			,status           =coalesce(v_status            ,etl_log.status            )
			,log_level        =coalesce(v_log_level         ,etl_log.log_level         )
			,log_file         =coalesce(v_log_file          ,etl_log.log_file          )
			,file_row_count   =coalesce(v_file_row_count    ,etl_log.file_row_count    )
			,process_row_count=coalesce(v_process_row_count ,etl_log.process_row_count )
			,error_row_count  =coalesce(v_error_row_count   ,etl_log.error_row_count   )
			,end_date         =coalesce(v_end_date          ,etl_log.end_date          )
		where log_id = v_log_Id;

	else

		insert into shrx.etl_log (job_id,file_name,status,log_level,log_file,file_row_count,process_row_count,error_row_count,end_date)
		values
			(v_job_id,v_file_name,v_status,v_log_level,v_log_file,v_file_row_count,v_process_row_count,v_error_row_count,v_end_date)
		RETURNING log_id into v_log_id;

	end if;

	return v_log_Id;

end;
$$;