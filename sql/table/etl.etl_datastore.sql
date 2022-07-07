drop table if exists etl.etl_datastore;
CREATE TABLE etl.etl_datastore (
	 datastore        text      not null primary key
	,create_date      timestamp not null default now()
	,update_date      timestamp not null default now()
);