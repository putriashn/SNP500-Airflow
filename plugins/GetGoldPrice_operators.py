import logging
from datetime import date
import pandas as pd
import requests
import json
from airflow.models import BaseOperator

log = logging.getLogger(__name__)

def get_goldprice(ti):
    access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x'
    tanggal = date.today().strftime("%Y-%m-%d")
    symbol = 'XAU'
    endpoint = 'open-high-low-close/'
    base_currency = 'USD'
    res = requests.get('https://commodities-api.com/api/'+endpoint+tanggal+'?access_key='+access_key+'&base='+base_currency+'&symbols='+symbol)
    high_gold = 1/res.json()['rates']['low']
    low_gold = 1/res.json()['rates']['high']
    print(high_gold, low_gold)
    ti.xcom_push(key='high_gold', value=high_gold)
    ti.xcom_push(key='low_gold', value=low_gold)