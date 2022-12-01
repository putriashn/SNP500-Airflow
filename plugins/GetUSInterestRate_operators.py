import logging
from datetime import date
import pandas as pd
import requests
import json
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


#api_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x'

class USInterestRate(BaseOperator):
    def __init__(
        self, 
        tanggal, 
        *args, **kwargs):
        
        super(USInterestRate, self).__init__(*args, **kwargs)
        self.tanggal= tanggal


    def execute(self, context):
        tanggal  = self.tanggal
        BASE_URL= 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/'
        ENDPOINT= 'v2/accounting/od/avg_interest_rates'
        PARAM='?fields=avg_interest_rate_amt,record_date&filter=record_date:gte:'+tanggal
        FULL_URL= BASE_URL+ENDPOINT+PARAM
        res = requests.get(FULL_URL)
        res.status_code
        interest = res.json()['data'][0]['avg_interest_rate_amt']
        print(interest)