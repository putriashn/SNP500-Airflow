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

class EuroExchange(BaseOperator):
    def __init__(
        self, 
        access_key, 
        symbol, 
        *args, **kwargs):
        
        super(EuroExchange, self).__init__(*args, **kwargs)
        self.access_key = access_key
        self.symbol = symbol


    def execute(self, context):
        access_key = self.access_key
        symbol = self.symbol
        access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x'
        endpoint = 'latest'
        base_currency = 'USD'
        res = requests.get('https://commodities-api.com/api/'+endpoint+'?access_key='+access_key+'&base='+base_currency+'&symbols='+symbol)
        euro_rate = res.json()['data']['rates']['EUR']
        print(euro_rate)