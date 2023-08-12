from datetime import datetime
from typing import Dict, List
from psycopg.rows import dict_row
import json

from lib import PgConnect

class CourierReader:
    def __init__(self, pg: PgConnect) -> None:
        self.pg_client = pg.client()

    def get_couriers(self, load_threshold: datetime) -> List[Dict]:

        resultlist = []

        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.deliverysystem_couriers
                    WHERE update_ts > %(load_threshold)s;
                """,
                {"load_threshold": load_threshold},
            )
            rows_list = cur.fetchall()

            for row in rows_list:
                courier_dict = json.loads(row['object_value'])
                resultlist.append({'courier_id': row['object_id'], 
                                    'name': courier_dict['name'], 
                                    'update_ts': row['update_ts']})

        return resultlist
