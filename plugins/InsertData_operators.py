import logging
from datetime import date
# Libraries
import pip

def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])

install('psycopg2')

import psycopg2 as pg
from airflow.models import BaseOperator

log = logging.getLogger(__name__)

def insert_todatabase(ti):
    harini = str(date.today())
    print(harini)
    gold_high = round(ti.xcom_pull(task_ids='get_goldprice', key='high_gold'),2)
    gold_low = round(ti.xcom_pull(task_ids='get_goldprice', key='low_gold'),2)
    oil_high = round(ti.xcom_pull(task_ids='get_oilprice', key='high_oil'),2)
    oil_low = round(ti.xcom_pull(task_ids='get_oilprice', key='low_oil'),2)
    bitcoin_high = round(ti.xcom_pull(task_ids='get_bitcoinprice', key='high_bitcoin'),2)
    bitcoin_low = round(ti.xcom_pull(task_ids='get_bitcoinprice', key='low_bitcoin'),2)
    euro_exchange = round(ti.xcom_pull(task_ids='get_euroexchangerate', key='euro_rate'),2)
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
                    """.format(harini, 4015, 4014, gold_high, gold_low, oil_high, oil_low, bitcoin_high, bitcoin_low, euro_exchange, interest))
    dbconnect.commit()       
        
    log.info("Inserted to database")
