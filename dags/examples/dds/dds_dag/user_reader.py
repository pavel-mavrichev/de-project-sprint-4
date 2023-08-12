from datetime import datetime
from typing import Dict, List
from pydantic import BaseModel
from psycopg import Connection
from psycopg.rows import class_row
from psycopg.rows import dict_row
import json

from lib import PgConnect

'''class StgOrdersystemUser(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: any'''

class UserReader:
    def __init__(self, pg: PgConnect) -> None:
        self.pg_client = pg.client()

    def get_users(self, load_threshold: datetime, limit) -> List[Dict]:

        resultlist = []

        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.ordersystem_users
                    WHERE update_ts > %(load_threshold)s;
                """,
                {"load_threshold": load_threshold},
            )
            rows_list = cur.fetchall()

            for row in rows_list:
                user_dict = json.loads(row['object_value'])
                resultlist.append({'user_id': row['object_id'], 
                                    'user_name': user_dict['name'], 
                                    'user_login': user_dict['login'], 
                                    'update_ts': row['update_ts']})

        return resultlist
