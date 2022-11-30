from datetime import datetime, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from GetGoldPrice_operators import GoldPrice
from GetOilPrice_operators import OilPrice
from GetBitcoinPrice_operators import BitcoinPrice
from GetEuroExchangeRate_operators import EuroExchange
from GetUSInterestRate_operators import USInterestRate
from InsertToDatabase_operators import InsertToDatabase


dag = DAG('get_gold_price', description='ambil gold price dari API',
          schedule_interval=None,
          start_date=datetime(2022, 11, 30), catchup=False)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

get_goldprice = GoldPrice(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'XAU',
                        task_id='get_gold_price_task', 
                        dag=dag)

get_oilprice = OilPrice(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'WTIOIL',
                        task_id='get_oil_price_task', 
                        dag=dag)

get_bitcoinprice = BitcoinPrice(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'BTC',
                        task_id='get_bitcoin_price_task', 
                        dag=dag)

get_euroexchangerate = EuroExchange(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'EUR',
                        task_id='get_euro_exchange_rate', 
                        dag=dag) 

get_USinterestrate = USInterestRate(access_key = '9q6ctgp5t2h2n52n821ah17ey52kxju76wupxdxmprp1al0yy4oi1vb1bf4x',
                        tanggal = '2011-11-29',
                        symbol = 'EUR',
                        task_id='get_US_interest_rate', 
                        dag=dag)  

insert_todatabase = InsertToDatabase(task_id='insert_to_database', 
                        dag=dag)

dummy_task >> get_goldprice >> get_oilprice >> get_bitcoinprice >> get_euroexchangerate >> get_USinterestrate >> insert_todatabase
