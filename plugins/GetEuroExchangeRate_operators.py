import logging
from datetime import date
import pandas as pd
import requests
import json
from airflow.models import BaseOperator

log = logging.getLogger(__name__)

def get_euroexchangerate(ti):
    access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x'
    symbol = 'EUR'
    access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x'
    endpoint = 'latest'
    base_currency = 'USD'
    res = requests.get('https://commodities-api.com/api/'+endpoint+'?access_key='+access_key+'&base='+base_currency+'&symbols='+symbol)
    euro_rate = res.json()['data']['rates']['EUR']
    print(euro_rate)
    ti.xcom_push(key='euro_rate', value=euro_rate)