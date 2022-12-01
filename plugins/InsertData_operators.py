# Libraries
import pip

def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])

install('psycopg2')

import logging
from datetime import date

import psycopg2 as pg
from airflow.models import BaseOperator

log = logging.getLogger(__name__)

def insert_todatabase(ti):
    gold_high = ti.xcom_pull(task_ids='get_goldprice', key='high_gold')
    gold_low = ti.xcom_pull(task_ids='get_goldprice', key='low_gold')
    oil_high = ti.xcom_pull(task_ids='get_oilprice', key='high_oil')
    oil_low = ti.xcom_pull(task_ids='get_oilprice', key='low_oil')
    bitcoin_high = ti.xcom_pull(task_ids='get_bitcoinprice', key='high_bitcoin')
    bitcoin_low = ti.xcom_pull(task_ids='get_bitcoinprice', key='low_bitcoin')
    euro_exchange = ti.xcom_pull(task_ids='get_euroexchangerate', key='euro_rate')
    interest = ti.xcom_pull(task_ids='get_USinterestrate', key='interest')

    try:
        dbconnect = pg.connect(
        "dbname='airflow' user='airflow' host='snp500-airflow-postgres-1' password='airflow'"
        )
    except Exception as error:
        print(error)
    
    # insert values into table
    cursor = dbconnect.cursor()
    cursor.execute("""
                    INSERT INTO sumberdata
                    VALUES ({},{},{},{},{},{},{},{},{},{},{})
                    """.format(date.today().strftime("%YYYY-%MM-%DD"), 4015, 4014, gold_high, gold_low, oil_high, oil_low, bitcoin_high, bitcoin_low, euro_exchange, interest))
    dbconnect.commit()       
        
    log.info("Inserted to database")
