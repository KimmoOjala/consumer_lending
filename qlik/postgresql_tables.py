from __future__ import (absolute_import, division, print_function, unicode_literals)
import pandas as pd
import psycopg2
import numpy as np
from psycopg2 import connect, sql, extras
from sqlalchemy import Column, MetaData, Table, create_engine
from sqlalchemy import String, Integer, Float, BigInteger, DateTime
from sqlalchemy.schema import DropTable, CreateTable
from sqlalchemy.orm import scoped_session, sessionmaker
from contextlib import contextmanager


# PostgreSQL on desktop server
database='bondora_db'
user='postgres'
password='salaatti'
hostname ='localhost'


def create_interest_df():
    conn = psycopg2.connect(database=database, user=user, password=password, host=hostname, port='5432')
    cur = conn.cursor() 
    cur.execute("SELECT status, interest FROM test;")
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['status', 'interest'])    
    interest_df = pd.DataFrame(columns=['cum_interest', 'interest_rate', 'status'])
    interest_rates = range(int(df.interest.min()), int(df.interest.max())+1)

    for status in df.status.unique():
        bin_df = pd.DataFrame()

        for i in interest_rates:
            bin_df[i] = np.where(df.loc[df['status'] == status]['interest'].values.round()==i, 1, 0)

        cum_df = pd.DataFrame(np.cumsum(np.array([bin_df[r].sum() for r in interest_rates]))/len(df.loc[df['status']==status]), columns=['cum_interest'])
        cum_df['interest_rate'] = interest_rates
        cum_df['status'] = status
        interest_df = interest_df.append(cum_df)
    
    cur.close()
    conn.close()
    
    return interest_df

def create_default_rate_df():
    conn = psycopg2.connect(database=database, user=user, password=password, host=hostname, port='5432')
    cur = conn.cursor() 
    cur.execute("SELECT loandate, loanduration, defaultdate FROM test;")
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['loandate', 'loanduration', 'defaultdate'])
    #df.defaultdate = df.defaultdate.apply(lambda x: pd.to_datetime(x, format='%Y-%m-%dT%H:%M:%S'))
    df.loandate = df.loandate.apply(lambda x: pd.to_datetime(x, format='%Y-%m-%dT%H:%M:%S'))
    df['loanYear'] = df['loandate'].map(lambda x: x.year)
    years = df['loanYear'].unique()
    year_loanduration_d = {}
    for i in years:
        durations = df.loanduration.unique()
        durations.sort()
        year_loanduration_d[i] = durations
    combinations = []
    
    for k,v in year_loanduration_d.items():
        for i in v:
            all_loans = len(df.loc[(df['loanYear'] == k) & (df['loanduration'] == i)])
            late_loans = len(df.loc[(df['loanYear'] == k) & (df['loanduration'] == i) & (~df.defaultdate.isnull())])
            if all_loans != 0:
                late_ratio = np.around(late_loans/all_loans, 2)
                combinations.append([k, i, late_ratio])
            else:
                combinations.append([k, i, np.nan])
    
    years = []
    loandurations = []
    late_ratios = []

    for i in combinations:
        years.append(i[0])
        loandurations.append(i[1])
        late_ratios.append(i[2])
        
    default_rate_df = pd.DataFrame({'loanyear':years, 'loanduration':loandurations, 'default_rate':late_ratios})
    return default_rate_df

# https://gist.github.com/djrobstep/998b9779d0bbcddacfef5d76a3d0921a

@contextmanager
def Session():
    Session = scoped_session(sessionmaker(
        bind=create_engine(f'postgresql+psycopg2://{user}:{password}@{hostname}/{database}')))

    try:
        session = Session()
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

def create_table(TABLE_SPEC, TABLE_NAME):    
    columns = [Column(n, t) for n, t in TABLE_SPEC]
    table = Table(TABLE_NAME, MetaData(), *columns)

    with Session() as s:
        # this is just here to make the script idempotent
        s.execute('drop table if exists {}'.format(TABLE_NAME))

        table_creation_sql = CreateTable(table)
        s.execute(table_creation_sql)

def main():
    '''Creates  default_rate_df. Derives columns for PostgreSQL table from heat_df and creates table.
    Inserts rows of heat_df into the table.'''
    conn = psycopg2.connect(database=database, user=user, password=password, host=hostname, port='5432')
    cur = conn.cursor() 

    # cum_interest_rates table
    TABLE_SPEC = [
        ('cum_interest', Float),
        ('interest_rate', Integer),
        ('status', String)
        ]
        
    TABLE_NAME = 'cum_interest_rates'
    create_table(TABLE_SPEC, TABLE_NAME)
    
    df = create_interest_df()
    rows_as_tuples = [row for row in df.itertuples(index=False)] 
    insert_query = 'insert into cum_interest_rates values %s'
    psycopg2.extras.execute_values(
    cur, insert_query, rows_as_tuples, template=None, page_size=100)
    conn.commit()
    
    # default_rate table
    TABLE_SPEC = [
        ('default_rate', Float),
        ('loanduration', Integer),
        ('loanyear', Integer)]
        
    TABLE_NAME = 'default_rate'
    create_table(TABLE_SPEC, TABLE_NAME)
    
    df = create_default_rate_df()
   
    rows_as_tuples = [row for row in df.itertuples(index=False)] 
    insert_query = 'insert into default_rate values %s'
    psycopg2.extras.execute_values(
    cur, insert_query, rows_as_tuples, template=None, page_size=100)
    conn.commit()

if __name__ == '__main__':
    main()       