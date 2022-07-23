
select shrx.etl_job_upsert(
 v_job_name        =>'MSOT_WALMART'
,v_is_active       =>'t'
,v_partner         =>'WALMART'
,v_job_type        =>'inbound'
,v_source_loc      =>null
,v_destination_loc =>null
,v_file_pattern    =>'Walmart_MSOT_Daily_20220601.csv'
,v_entrypoint      =>'inbound_single_file'
,v_processes       =>'load_dataframe,appt_match,msot'
,v_notes           =>null
,v_sumologic       =>null
,v_config_ext      =>'{"load_dataframe_config":{"dfName":"NicksDataFrame"}}'
);


select shrx.etl_job_upsert(
 v_job_name        =>'APPOINTMENT_WALMART'
,v_is_active       =>'t'
,v_partner         =>'WALMART'
,v_job_type        =>'inbound'
,v_source_loc      =>'//_processing/{env}/externalData/cooper/incoming'
,v_destination_loc =>null
,v_file_pattern    =>'Walmart_FILL_Daily_20220601.csv'
,v_entrypoint      =>'inbound_single_file'
,v_processes       =>'load_dataframe,mask_phi,fill'
,v_notes           =>null
,v_sumologic       =>null
,v_config_ext      =>'{"msot_config": {"DateStyle": "YYYYMMDD", "Headers": ["First_Name", "Last_Name"] }, "load_dataframe_config": {"dfName": "FillDataFrame", "sep": "|"} }'
);


