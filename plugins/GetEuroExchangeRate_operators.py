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
        tanggal, 
        *args, **kwargs):
        
        super(EuroExchange, self).__init__(*args, **kwargs)
        self.access_key = access_key
        self.symbol = symbol
        self.tanggal= tanggal


    def execute(self, context):
        access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x'
        endpoint = 'latest'
        base_currency = 'USD'
        symbol = 'EUR'
        res = requests.get('https://commodities-api.com/api/'+endpoint+'?access_key='+access_key+'&base='+base_currency+'&symbols='+symbol)
        response_API = res.json()
        print(response_API)



# def call_high_low_api(access_key, symbol, date):
#     endpoint = 'open-high-low-close/'+date 
#     base_currency = 'USD'
#     response_API = requests.get('https://commodities-api.com/api/'+endpoint+'?access_key='+access_key+'&base='+base_currency+'&symbols='+symbol)
#     return {"high" : 1/response_API.json()['rates']['high'], "low" : 1/response_API.json()['rates']['low']} 

# def get_euro_exchange_rate(date):
#     endpoint = 'latest'
#     base_currency = 'USD'
#     access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x'
#     symbol = 'EUR'
#     response_API = requests.get('https://commodities-api.com/api/'+endpoint+'?access_key='+access_key+'&base='+base_currency+'&symbols='+symbol)
#     return {'EURO_To_USD_Rate': response_API.json()['data']['rates']['EUR']}

# def get_gold_price(date):
#     return call_high_low_api(api_key,'XAU', date)