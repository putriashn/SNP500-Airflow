import logging
from datetime import date
import pandas as pd
import requests
import json
from airflow.models import BaseOperator

log = logging.getLogger(__name__)

def get_USinterestrate(ti):
    tanggal = '2022-10-1'
    BASE_URL= 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/'
    ENDPOINT= 'v2/accounting/od/avg_interest_rates'
    PARAM='?fields=avg_interest_rate_amt,record_date&filter=record_date:gte:'+tanggal
    FULL_URL= BASE_URL+ENDPOINT+PARAM
    res = requests.get(FULL_URL)
    res.status_code
    interest = res.json()['data'][0]['avg_interest_rate_amt']
    print(interest)
    ti.xcom_push(key='interest', value=interest)