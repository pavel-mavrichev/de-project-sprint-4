from datetime import datetime
from typing import Dict, List
from psycopg.rows import dict_row
import json

from lib import PgConnect

class TimestampReader:
    def __init__(self, pg: PgConnect) -> None:
        self.pg_client = pg.client()

    def get_timestamp(self, load_threshold: datetime, limit) -> List[Dict]:

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
                timestamp_dict = json.loads(row['object_value'])
                date = datetime.fromisoformat(timestamp_dict['date'])
                resultlist.append({'ts': date, 
                                    'year': date.year, 
                                    'month': date.month,
                                    'day': date.day, 
                                    'date': date.date(),
                                    'time': date.time(),
                                    'update_ts': row['update_ts']})

        return resultlist
