import os
import psycopg2
from psycopg2 import extras
import pandas as pd
from sqlalchemy import create_engine

connstr = "dbname='{}' user='{}' host='{}' password='{}'" \
    .format(os.getenv("PG_DBNAME"), os.getenv("PG_USER"), os.getenv("PG_HOST"), os.getenv("PG_PWD"))


def select(sql):
    conn = psycopg2.connect(connstr)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def select_df(sql):
    try:
        conn = psycopg2.connect(connstr)
        df = pd.read_sql_query(sql, conn)
        return df
    except (Exception, psycopg2.Error) as error:
        print("Failed to get dataframe", error)
    finally:
        conn.close()

def select_dict(sql):
    try:
        conn = psycopg2.connect(connstr)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        return rows
    except (Exception, psycopg2.Error) as error:
        print(str(error))
    finally:
        conn.close()

def insert_data(sql, data):
    try:
        conn = psycopg2.connect(connstr)
        cur = conn.cursor()
        extras.execute_values(cur, sql, data)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into mobile table", error)
    finally:
        if conn:
            cur.close()
            conn.close()

def execute_sql(sql):
    conn = psycopg2.connect(connstr)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def insert_df(df, schema, table):
    engine = create_engine(f'postgresql://{os.getenv("PG_USER")}:{os.getenv("PG_PWD")}@{os.getenv("PG_HOST")}:5432/{os.getenv("PG_DBNAME")}')
    df.to_sql(table, engine, schema=schema, if_exists='append', index=False)
