#sample deploy
import os
import datapump_utils as sfu
from pathlib import Path

env = os.environ['ENV']
env='test'

sql = open(str(Path(__file__).parent.parent) + "/modules/msot/sql/test.sql", "r").read()
# sql='SELECT pg_size_pretty( pg_database_size('albanydev') )'

result_list={}

for datastore,info in sfu.datastores[env].items():
	if info.get('is_partner')!=True:
		continue

	db=sfu.Database.easy_connect(env=env,datastore=datastore)
	try:
		print('...trying ',datastore)
		rst=db.execute(f"SELECT pg_size_pretty( pg_database_size('{info['database']}') )")
		result_list[datastore]=rst
	except:
		print(datastore,'failed')

print('f')
