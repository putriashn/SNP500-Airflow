from datetime import datetime, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator

from GetGoldPrice_operators import get_goldprice
from GetOilPrice_operators import get_oilprice
from GetBitcoinPrice_operators import get_bitcoinprice
from GetEuroExchangeRate_operators import get_euroexchangerate
from GetUSInterestRate_operators import get_USinterestrate
from InsertData_operators import insert_todatabase

dag = DAG('all_data', description='ambil seluruh data dari API',
          schedule_interval=None,
          start_date=datetime(2022, 11, 30), catchup=False)

start_task = DummyOperator(task_id='start_task', dag=dag)

get_goldprice = PythonOperator(
                                task_id='get_goldprice', 
                                python_callable=get_goldprice,
                                dag=dag
                              )

get_oilprice = PythonOperator(
                              task_id='get_oilprice',
                              python_callable=get_oilprice, 
                              dag=dag
                              )

get_bitcoinprice = PythonOperator(
                                  task_id='get_bitcoinprice', 
                                  python_callable=get_bitcoinprice, 
                                  dag=dag
                                  )

get_euroexchangerate = PythonOperator(
                                      task_id='get_euroexchangerate', 
                                      python_callable=get_euroexchangerate,
                                      dag=dag
                                      ) 

get_USinterestrate = PythonOperator(
                                    task_id='get_USinterestrate', 
                                    python_callable=get_USinterestrate,
                                    dag=dag
                                    )

insert_todatabase = PythonOperator(
                                    task_id='insert_to_database',
                                    python_callable=insert_todatabase,
                                    dag=dag
                                    )

start_task >> get_goldprice >> get_oilprice >> get_bitcoinprice >> get_euroexchangerate >> get_USinterestrate >> insert_todatabase