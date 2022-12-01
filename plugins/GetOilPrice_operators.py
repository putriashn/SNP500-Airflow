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

class OilPrice(BaseOperator):
    def __init__(
        self, 
        access_key, 
        symbol, 
        tanggal, 
        *args, **kwargs):
        
        super(OilPrice, self).__init__(*args, **kwargs)
        self.access_key = access_key
        self.symbol = symbol
        self.tanggal= tanggal


    def execute(self, context):
        access_key = self.access_key
        tanggal = self.tanggal
        symbol = self.symbol
        endpoint = 'open-high-low-close/'
        base_currency = 'USD'
        res = requests.get('https://commodities-api.com/api/'+endpoint+tanggal+'?access_key='+access_key+'&base='+base_currency+'&symbols='+symbol)
        high_oil = 1/res.json()['rates']['low']
        low_oil = 1/res.json()['rates']['high']
        print(high_oil, low_oil)