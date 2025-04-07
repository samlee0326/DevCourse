import configparser
import psycopg2
from sql_queries import create_schema_queries,create_table_queries,staging_table_queries
import os

def create_schema(cur,conn):
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur,conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def stage_tables(cur,conn):
    for query in staging_table_queries:
        cur.execute(query)
        conn.commit()

def rs_load():
    """
    AWS에 테이블 생성 및 데이터 추가
    """

    #현재 파일의 상위 폴더 위치
    PARENT_DIR = os.path.dirname(os.getcwd())

    config = configparser.ConfigParser()
    config.read(os.path.join(PARENT_DIR,'config','conf.cfg'))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['SERVERLESS'].values()))
    print('Connected')

    cur = conn.cursor()
    print()
    print('Cursor Created')
    print()

    print('Creating Schema...')
    create_schema(cur, conn)
    print()

    print('Creating Tables')
    create_tables(cur, conn)
    print()

    print('Staging Tables')
    stage_tables(cur,conn)

    conn.close()

    print()
    print('Connection Closed')

if __name__ == '__main__':
    rs_load()