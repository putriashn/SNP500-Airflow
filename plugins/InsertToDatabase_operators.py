import logging
from datetime import date
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from GetGoldPrice_operators import GoldPrice
from GetOilPrice_operators import OilPrice
from GetBitcoinPrice_operators import BitcoinPrice
from GetEuroExchangeRate_operators import EuroExchange
from GetUSInterestRate_operators import USInterestRate



log = logging.getLogger(__name__)

class getQuery(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(getQuery, self).__init__(*args, **kwargs)
    def execute(self, context):
        res = GoldPrice(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'XAU')
        gold_high = 1/res.json()['rates']['high']
        res = GoldPrice(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'XAU')
        gold_low = 1/res.json()['rates']['low']
        res = OilPrice(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'XAU')
        oil_high = 1/res.json()['rates']['high']
        res = OilPrice(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'XAU')
        oil_low = 1/res.json()['rates']['low']
        res = BitcoinPrice(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'XAU')
        bitcoin_high = 1/res.json()['rates']['high']
        res = BitcoinPrice(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'XAU')
        bitcoin_low = 1/res.json()['rates']['low']
        res = EuroExchange(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'EUR')
        euro_exchange = res.json()['data']['rates']['EUR']
        res = USInterestRate(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'EUR')
        us_interestRate = res.json()['data'][0]['avg_interest_rate_amt']
        
        query = date.today().strftime("%Y-%m-%d") + '4015, 4014,'+ gold_high + ',' + gold_low + ',' + oil_high + ',' + oil_low + ',' + bitcoin_high + ',' + bitcoin_low + ',' + euro_exchange + ',' + us_interestRate


class InsertToDatabase(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(InsertToDatabase, self).__init__(*args, **kwargs)
    def execute(self, context):
        log.info("Inserted to database")
        
    