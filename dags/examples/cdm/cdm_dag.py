import logging
import datetime

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_cdm():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def update_courier_ledger():

        today = datetime.date.today()
        first = today.replace(day=1)
        last_month = first - datetime.timedelta(days=1)
        first_day_last_month = last_month.replace(day=1)
        log.info(first_day_last_month)

        with open('/lessons/dags/examples/cdm/sql/dm_courier_ledger-delete.sql', 'r') as f:
            delete_script = f.read()

        with open('/lessons/dags/examples/cdm/sql/dm_courier_ledger-fill.sql', 'r') as f:
            insert_script = f.read()

        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(delete_script,
                    {
                        "first_day_last_month": first_day_last_month
                    })
                cur.execute(insert_script,
                    {
                        "first_day_last_month": first_day_last_month
                    })
   
    update_courier_ledger_task = update_courier_ledger()


    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    update_courier_ledger_task 


cdm_dag = sprint5_cdm()  # noqa
