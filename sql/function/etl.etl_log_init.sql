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
