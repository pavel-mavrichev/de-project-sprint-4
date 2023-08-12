from datetime import datetime
from typing import Dict, List
from psycopg.rows import dict_row
import json

from lib import PgConnect

class SalesReader:
    def __init__(self, pg: PgConnect, pg_bonus: PgConnect) -> None:
        self.pg_client = pg.client()
        self.pg_bonus_client = pg_bonus.client()

    def get_sales(self, load_threshold: datetime, limit, log) -> List[Dict]:

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
                items_list = order_dict['order_items']
                for item in items_list:
                    bonus_info = self.get_bonus_info(order_dict['_id'], item['id'])
                    resultlist.append({'product_id': self.get_product(item['id']), 
                                        'order_id': self.get_order(order_dict['_id']), 
                                        'count': item['quantity'],
                                        'price': item['price'], 
                                        'total_sum': item['quantity'] * item['price'],
                                        'bonus_payment': bonus_info['payment_sum'],
                                        'bonus_grant': bonus_info['granted_sum'],
                                        'update_ts': row['update_ts']})

        return resultlist

    def get_product(self, product_id):
        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute('select id from dds.dm_products where product_id = %(product_id)s;',
                {"product_id": product_id},
            )
            row = cur.fetchone()
            return row['id']

    def get_order(self, order_key):
        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute('select id from dds.dm_orders where order_key = %(order_key)s;',
                {"order_key": order_key},
            )
            row = cur.fetchone()
            return row['id']

    def get_bonus_info(self, order_key, product_id):
        with self.pg_bonus_client.cursor(row_factory=dict_row) as cur:
            cur.execute('select payment_sum, granted_sum from public.bonus_transactions where order_id = %(order_key)s and product_id = %(product_id)s;',
                {"order_key": order_key,
                "product_id": product_id}
            )
            row = cur.fetchone()
            if row is None:
                row = {'payment_sum': 0, 'granted_sum': 0}
            return row

    


