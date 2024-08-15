from airflow.decorators import dag, task
from datetime import datetime
from airflow.models.baseoperator import chain
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.operators.python import PythonOperator
from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_prices_from_minio
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
import requests


SYMBOL='AAPL'

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    
)
def stock_market():
    @task.sensor(poke_interval=30, timeout=3600, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None 
        return PokeReturnValue(is_done=condition, xcom_value=url)
    

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL},
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'prices': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'},
    )

    is_api_available() >> get_stock_prices >> store_prices
stock_market()