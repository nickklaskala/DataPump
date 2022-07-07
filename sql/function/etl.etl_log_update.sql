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