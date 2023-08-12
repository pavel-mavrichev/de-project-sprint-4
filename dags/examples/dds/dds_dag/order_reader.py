from datetime import datetime
from typing import Dict, List
from psycopg.rows import dict_row
import json

from lib import PgConnect

class OrderReader:
    def __init__(self, pg: PgConnect) -> None:
        self.pg_client = pg.client()

    def get_order(self, load_threshold: datetime, limit) -> List[Dict]:

        resultlist = []

        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.ordersystem_orders
                    WHERE update_ts > %(load_threshold)s;
                """,
                {"load_threshold": load_threshold},
            )
            rows_list = cur.fetchall()

            for row in rows_list:
                order_dict = json.loads(row['object_value'])
                resultlist.append({'user_id': self.get_user(order_dict['user']['id']), 
                                    'restaurant_id': self.get_restaurant(order_dict['restaurant']['id']), 
                                    'timestamp_id': self.get_timestamp(order_dict['date']),
                                    'order_key': order_dict['_id'], 
                                    'order_status': order_dict['final_status'],
                                    'update_ts': row['update_ts']})

        return resultlist

    def get_restaurant(self, restaurant_id):
        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute('select id from dds.dm_restaurants where restaurant_id = %(restaurant_id)s;',
                {"restaurant_id": restaurant_id},
            )
            row = cur.fetchone()
            return row['id']

    def get_user(self, user_id):
        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute('select id from dds.dm_users where user_id = %(user_id)s;',
                {"user_id": user_id},
            )
            row = cur.fetchone()
            return row['id']

    def get_timestamp(self, ts):
        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute('select id from dds.dm_timestamps where ts = %(ts)s;',
                {"ts": ts},
            )
            row = cur.fetchone()
            return row['id']

