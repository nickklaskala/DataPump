
select etl.etl_job_upsert(
 v_job_name        =>'MSOT_WALMART'
,v_is_active       =>'t'
,v_datastore        =>'WALMART'
,v_job_type        =>'tiggered-file-drop'
,v_date_signature  =>null
,v_source_loc      =>'C:/data/walmart'
,v_destination_loc =>null
,v_file_pattern    =>'Walmart_MSOT_Daily_20220601.csv'
,v_entrypoint      =>'inbound_single_file'
,v_processes       =>'load_dataframe,appointments'
,v_notes           =>null
,v_config_ext      =>'{"load_dataframe_config":{"df_name":"NicksDataFrame"}}'
);




select etl.etl_job_upsert(
 v_job_name        =>'APPOINTMENT_WALMART'
,v_is_active       =>'t'
,v_datastore         =>'WALMART'
,v_job_type        =>'scheduled'
,v_date_signature  =>'w-1111100-1130,1230,1700'
,v_source_loc      =>'//_processing/{env}/externalData/cooper/incoming'
,v_destination_loc =>null
,v_file_pattern    =>'Walmart_FILL_Daily_20220601.csv'
,v_entrypoint      =>'inbound_single_file'
,v_processes       =>'load_dataframe,mask_phi,fill'
,v_notes           =>null 
,v_config_ext      =>'{"msot_config": {"DateStyle": "YYYYMMDD", "Headers": ["First_Name", "Last_Name"] }, "load_dataframe_config": {"dfName": "FillDataFrame", "sep": "|"} }'
);

