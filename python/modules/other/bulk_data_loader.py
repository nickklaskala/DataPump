import pandas as pd
import datapump_utils as sfu

db1=sfu.Database.easy_connect_sqlalchemy('prod','WALMART')
db2=sfu.Database.easy_connect_sqlalchemy('test','WALMART')

df=pd.read_sql_table(schema='sales',table_name='sales_volume', con=db1)
df.to_sql(name='sales_volume_backup2', schema='sales', con=db2 ,if_exists='append', index=False,chunksize=2000)
