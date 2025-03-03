#sample deploy
import os
import datapump_utils as sfu
from pathlib import Path

env = os.environ['ENV']
sql1 = open(str(Path(__file__).parent.parent) + "/projects/IN-9999/IN-9999.sql", "r").read()
sql2 = open(str(Path(__file__).parent.parent) + "/projects/IN-9999/IN-9999v2.sql", "r").read()


db=sfu.Database.easy_connect(env=env,datastore='MY_PARTNER_IM_DOING_MAINTENANCE_TOO')
rst=db.execute(sql1)
rst=db.execute(sql2)
