"""
Expected environment variables (beyond those in real code):
  TESTUSER (valid for SHRX)
  TESTPASSWORD (valid for SHRX)
"""

import os
import psycopg2
import pytest
import datapump_utils

TEST_ENV = 'dev'

def test_easy_connect_SHRX_user_and_pass_from_environ():
	try:
		test_db = datapump_utils.Datastore.easy_connect(TEST_ENV, 'SHRX', False)
		assert isinstance(test_db, datapump_utils.Datastore)
		assert isinstance(test_db.connection, datapump_utils.Datastore.Postgres)
		assert isinstance(test_db.connection.connection, psycopg2.extensions.connection)
		assert isinstance(test_db.connection.cursor, psycopg2.extensions.cursor)
		assert isinstance(test_db.connection.cursor_dict, psycopg2.extras.RealDictCursor)
		with pytest.raises(AttributeError):
			_ = test_db.connection.autocommit
		assert test_db.connection.connection.autocommit is False
		assert test_db.connection.connection.closed == 0
		test_db_params = test_db.connection.connection.get_dsn_parameters()
		assert test_db_params['dbname'] == datapump_utils.datastores[TEST_ENV]['SHRX']['database']
		assert test_db_params['host'] == datapump_utils.datastores[TEST_ENV]['SHRX']['host']
		assert test_db_params['port'] == datapump_utils.datastores[TEST_ENV]['SHRX']['port']
		assert test_db_params['user'] == os.environ['PGUSER']
	finally:
		try:
			test_db.connection.connection.close()
			assert test_db.connection.connection.closed == 1
		except UnboundLocalError:
			pass

def test_easy_connect_SHRX_user_and_pass_as_args():
	try:
		test_db = datapump_utils.Datastore.easy_connect(
											TEST_ENV,
											'SHRX',
											False,
											user=os.environ['TESTUSER'],
											password=os.environ['TESTPASSWORD'])
		assert isinstance(test_db, datapump_utils.Datastore)
		assert isinstance(test_db.connection, datapump_utils.Datastore.Postgres)
		assert isinstance(test_db.connection.connection, psycopg2.extensions.connection)
		assert isinstance(test_db.connection.cursor, psycopg2.extensions.cursor)
		assert isinstance(test_db.connection.cursor_dict, psycopg2.extras.RealDictCursor)
		with pytest.raises(AttributeError):
			_ = test_db.connection.autocommit
		assert test_db.connection.connection.autocommit is False
		assert test_db.connection.connection.closed == 0
		test_db_params = test_db.connection.connection.get_dsn_parameters()
		assert test_db_params['dbname'] == datapump_utils.datastores[TEST_ENV]['SHRX']['database']
		assert test_db_params['host'] == datapump_utils.datastores[TEST_ENV]['SHRX']['host']
		assert test_db_params['port'] == datapump_utils.datastores[TEST_ENV]['SHRX']['port']
		assert test_db_params['user'] == os.environ['TESTUSER']
	finally:
		try:
			test_db.connection.connection.close()
			assert test_db.connection.connection.closed == 1
		except UnboundLocalError:
			pass

def test_Maestro_init():
	try:
		test_mae = datapump_utils.Maestro(TEST_ENV)
		assert isinstance(test_mae.db_shrx, datapump_utils.Datastore)
		assert isinstance(test_mae.db_shrx.connection, datapump_utils.Datastore.Postgres)
		assert test_mae.db_shrx.connection.connection.closed == 0
		test_mae_params = test_mae.db_shrx.connection.connection.get_dsn_parameters()
		assert test_mae_params['dbname'] == datapump_utils.datastores[TEST_ENV]['SHRX']['database']
		assert test_mae_params['host'] == datapump_utils.datastores[TEST_ENV]['SHRX']['host']
		assert test_mae_params['port'] == datapump_utils.datastores[TEST_ENV]['SHRX']['port']
		assert test_mae_params['user'] == os.environ['PGUSER']
	finally:
		try:
			test_mae.db_shrx.connection.connection.close()
			assert test_mae.db_shrx.connection.connection.closed == 1
		except UnboundLocalError:
			pass
