import src.data_service.data_table_adaptor as dta
import src.data_service.RDBDataTable as rdb
import pymysql
import json

connect_info = {
    "host": 'localhost',
    "port": 3306,
    "user": 'root',
    "password": 'dbuserdbuser',
    "db": 'lahman2019clean',
    }


def t_get_db():
    db = dta.get_databases()
    print(json.dumps(db, indent=2))


def t_get_tables(dbname):
    tables = dta.get_tables(dbname)
    print(json.dumps(tables, indent=2))


def t_row_counts():
    db = rdb.RDBDataTable(table_name="people", db_name="lahman2019clean", connect_info=connect_info)
    print(db.get_row_count())

def t_get_keys():
    db = rdb.RDBDataTable(table_name="appearances", db_name="lahman2019clean", connect_info=connect_info)
    print(db.get_primary_key_columns())

#t_get_db
#t_get_tables("lahman2019clean")
#t_row_counts()
#t_get_keys()