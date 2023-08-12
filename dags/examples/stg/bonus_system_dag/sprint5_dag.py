import logging

import pendulum
from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from lib.dict_util import json2str
import pandas as pd
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

def download_upload_ranks():

    bonus_db_conn_id = PostgresHook('PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    wh_conn_id = PostgresHook('PG_WAREHOUSE_CONNECTION')

    engine = bonus_db_conn_id.get_sqlalchemy_engine()
    ranks_df = pd.read_sql('SELECT * FROM public.ranks', con=engine)

    wh_schema = 'stg'
    wh_table = 'bonussystem_ranks'

    engine = wh_conn_id.get_sqlalchemy_engine()
    engine.execute('delete from stg.bonussystem_ranks')
    ranks_df.to_sql(wh_table, engine, schema=wh_schema, if_exists='append', index=False)

def download_upload_users():

    bonus_db_conn_id = PostgresHook('PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    wh_conn_id = PostgresHook('PG_WAREHOUSE_CONNECTION')

    engine = bonus_db_conn_id.get_sqlalchemy_engine()
    ranks_df = pd.read_sql('SELECT * FROM public.users', con=engine)

    wh_schema = 'stg'
    wh_table = 'bonussystem_users'

    engine = wh_conn_id.get_sqlalchemy_engine()
    engine.execute('delete from stg.bonussystem_users')
    ranks_df.to_sql(wh_table, engine, schema=wh_schema, if_exists='append', index=False)

def download_upload_outbox():

    wh_conn_id = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    bonus_db_conn_id = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    log.info('1')

    settings_repository = StgEtlSettingsRepository()

    log.info('2')

    settings = settings_repository.get_setting(wh_conn_id.client(), 'events_load_settings')

    log.info('3')

    if not settings:
        settings = EtlSetting(id=0, workflow_key='events_load_settings', workflow_settings={'last_loaded_id': -1})

    log.info('4')
    log.info(str(settings))

    last_loaded = settings.workflow_settings['last_loaded_id']

    log.info('5')

    outbox_df = pd.read_sql(f'select id, event_ts, event_type, event_value from public.outbox where id > {last_loaded};', con=bonus_db_conn_id.client())

    log.info('6')

    wh_schema = 'stg'
    wh_table = 'bonussystem_events'

    with wh_conn_id.client() as conn:

        log.info('7')

        max_id = -1
        for index, row in outbox_df.iterrows():
            log.info(str(row))

            #print(row['c1'], row['c2'])
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                        VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s);
                    """,
                    {
                        "id": row['id'],
                        "event_ts": row['event_ts'],
                        "event_type": row['event_type'],
                        "event_value": row['event_value']
                    },
                )

            if max_id < row['id']:
                max_id = row['id']

        #outbox_df.to_sql(wh_table, conn, schema=wh_schema, if_exists='append', index=False)
        log.info('8')

        settings.workflow_settings['last_loaded_id'] = max_id #max([t.id for t in outbox_df])
        log.info('9')
        settings_json = json2str(settings.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
        log.info('10')
        settings_repository.save_setting(conn, 'events_load_settings', settings_json)
        log.info('11')

        

dag = DAG(
    dag_id='sprint5_stg_bonus_system_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'tasks'],
    is_paused_upon_creation=False
)

download_upload_ranks_task = PythonOperator(
    task_id='download_upload_ranks',
    python_callable=download_upload_ranks,
    dag=dag,
)

download_upload_users_task = PythonOperator(
    task_id='download_upload_users',
    python_callable=download_upload_users,
    dag=dag,
)

download_upload_outbox_task = PythonOperator(
    task_id='download_upload_outbox',
    python_callable=download_upload_outbox,
    dag=dag,
)

download_upload_ranks_task >> download_upload_users_task >> download_upload_outbox_task