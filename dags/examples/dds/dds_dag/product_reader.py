from datetime import datetime
from typing import Dict, List
from psycopg.rows import dict_row
import json

from lib import PgConnect

class ProductReader:
    def __init__(self, pg: PgConnect) -> None:
        self.pg_client = pg.client()

    def get_product(self, load_threshold: datetime, limit) -> List[Dict]:

        resultlist = []

        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE update_ts > %(load_threshold)s;
                """,
                {"load_threshold": load_threshold},
            )
            rows_list = cur.fetchall()

            for row in rows_list:
                restaurant_dict = json.loads(row['object_value'])
                menu_list = restaurant_dict['menu']
                for menu_item in menu_list:

                    resultlist.append({'product_id': menu_item['_id'], 
                                        'restaurant_id': self.get_restaurant(restaurant_dict['_id']), 
                                        'product_name': menu_item['name'],
                                        'product_price': menu_item['price'], 
                                        'active_from': row['update_ts'],
                                        'active_to': datetime.fromisoformat('2099-12-31 00:00:00'),
                                        'update_ts': row['update_ts']})

        return resultlist

    def get_restaurant(self, restaurant_id):
        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute('select id from dds.dm_restaurants where restaurant_id = %(restaurant_id)s;',
                {"restaurant_id": restaurant_id},
            )
            row = cur.fetchone()
            return row['id']

