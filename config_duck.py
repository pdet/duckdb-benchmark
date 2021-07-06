import duckdb

db = duckdb.connect("benchmark.db")


create_seq = ''' CREATE SEQUENCE IF NOT EXISTS seq;'''
db.execute(create_seq)

create_seq = ''' CREATE SEQUENCE IF NOT EXISTS seq_engine;'''
db.execute(create_seq)

create_table = ''' CREATE TABLE TEST (ID INTEGER DEFAULT nextval('seq'), ENGINE_ID INTEGER, NAME VARCHAR) '''
db.execute(create_table)

create_table = ''' CREATE TABLE RESULT (ID_TEST INTEGER, QUERY INTEGER, TIME_FIRST DOUBLE, TIME_SECOND DOUBLE) '''
db.execute(create_table)

create_table = ''' CREATE TABLE ENGINE (ID INTEGER DEFAULT nextval('seq_engine'), NAME VARCHAR, HASH VARCHAR) '''
db.execute(create_table)
