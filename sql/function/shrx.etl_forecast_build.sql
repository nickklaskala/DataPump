CREATE OR REPLACE FUNCTION shrx.etl_forecast_build()--select shrx.etl_forecast_build()
  RETURNS integer 
  LANGUAGE plpgsql
AS
$$
declare
	v_date_of_the_week integer = (SELECT EXTRACT(DOW FROM now())); 
	v_date_of_the_month integer = (select date_part('day', now())); 
begin

	---------build job and time of start
	drop table if exists tmp_forecasts;
	create temp table tmp_forecasts as 
	select 
		 job_id
		,job_name
--		,date_signature
--		,substring(date_signature,10) as time_signature 
--		,unnest(string_to_array(substring(date_signature,10),',')) as hour_minute 
		,(current_date::text||' '||unnest(string_to_array(substring(date_signature,10),',')))::timestamp start_datetime
	from shrx.etl_job 
	where job_type='scheduled' 
	and is_active=true
	and substring(date_signature,1,1) ilike 'w' 
	and substring(date_signature,1+v_date_of_the_week,1)='1'
	-----
	union 
	-----
	select 
		 job_id
		,job_name
--		,date_signature
--		,substring(date_signature,34) as time_signature 
--		,unnest(string_to_array(substring(date_signature,34),',')) as hour_minute 
		,(current_date::text||' '||unnest(string_to_array(substring(date_signature,34),',')))::timestamp start_datetime
	from shrx.etl_job 
	where job_type='scheduled' 
	and is_active=true
	and substring(date_signature,1,1) ilike 'm' 
	and substring(date_signature,1+v_date_of_the_month,1)='1'; 
 


	--------forecast jobs
	insert into shrx.etl_forecast (job_id,start_datetime,is_started)
	select
		 a.job_id 
 		,a.start_datetime
		,'f'
	from tmp_forecasts as a 
	left join shrx.etl_forecast as b on b.job_id =a.job_id and b.start_datetime=a.start_datetime
	where b.etl_forecast_id is null;




	-------remove unstarted jobs from forecast if schedule changes before start time
	with records_to_delete as (
		select 
			a.etl_forecast_id
		from shrx.etl_forecast as a 
		left join tmp_forecasts as b on b.job_id =a.job_id and b.start_datetime=a.start_datetime
		where a.is_started ='f' and b.job_id is null
	)
	delete from shrx.etl_forecast
	where etl_forecast_id in (select * from records_to_delete);
	

	-------remove job forecast if older than 1 month to maintain itself
	delete from shrx.etl_forecast
	where start_datetime<now()-interval '1 month';
	


	return 1;
end;
$$
;
   