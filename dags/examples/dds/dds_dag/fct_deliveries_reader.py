from datetime import datetime
from typing import Dict, List
from psycopg.rows import dict_row
import json

from lib import PgConnect

class DeliveryReader:
    def __init__(self, pg: PgConnect) -> None:
        self.pg_client = pg.client()

    def get_deliveries(self, load_threshold: datetime, limit, log) -> List[Dict]:

        resultlist = []

        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.deliverysystem_deliveries
                    WHERE update_ts > %(load_threshold)s;
                """,
                {"load_threshold": load_threshold},
            )
            rows_list = cur.fetchall()
            for row in rows_list:
                delivery_dict = json.loads(row['object_value'])
                resultlist.append({'delivery_id': delivery_dict['delivery_id'], 
                                    'courier_id': self.get_courier(delivery_dict['courier_id']), 
                                    'order_ts': delivery_dict['order_ts'],
                                    'order_id': self.get_order(delivery_dict['order_id']), 
                                    'rate': delivery_dict['rate'],
                                    'tip_sum':  delivery_dict['tip_sum'],
                                    'update_ts': row['update_ts']})

        return resultlist

    def get_courier(self, courier_id):
        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute('select id from dds.dm_couriers where courier_id = %(courier_id)s;',
                {"courier_id": courier_id},
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

    

    


