create table if not exists etl.etl_forecast 
(
	 etl_forecast_id serial
	,job_id bigint 
	,start_datetime timestamp
	,is_started bool
);

