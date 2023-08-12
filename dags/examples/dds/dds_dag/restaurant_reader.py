from datetime import datetime
from typing import Dict, List
from pydantic import BaseModel
from psycopg import Connection
from psycopg.rows import class_row
from psycopg.rows import dict_row
import json

from lib import PgConnect

class RestaurantReader:
    def __init__(self, pg: PgConnect) -> None:
        self.pg_client = pg.client()

    def get_restaurant(self, load_threshold: datetime, limit) -> List[Dict]:

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
                resultlist.append({'restaurant_id': row['object_id'], 
                                    'restaurant_name': restaurant_dict['name'], 
                                    'active_from': row['update_ts'],
                                    'active_to': datetime.fromisoformat('2099-12-31 00:00:00'), 
                                    'update_ts': row['update_ts']})

        return resultlist
