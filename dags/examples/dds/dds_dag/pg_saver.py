from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_user(self, conn: Connection, user_id: str, user_name: str, user_login: str):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                """,
                {
                    "user_id": user_id,
                    "user_name": user_name,
                    "user_login": user_login
                }
            )

    def save_restaurant(self, conn: Connection, restaurant_id: str, restaurant_name: str, active_from: datetime, active_to: datetime):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (restaurant_id) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "restaurant_id": restaurant_id,
                    "restaurant_name": restaurant_name,
                    "active_from": active_from,
                    "active_to": active_to
                }
            )

    def save_timestamp(self, conn: Connection, 
                        ts: datetime, 
                        year: int, 
                        month: int,
                        day: int,
                        time: datetime.time,
                        date: datetime.date):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (ts) DO UPDATE
                    SET
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        time = EXCLUDED.time,
                        date = EXCLUDED.date;
                """,
                {
                    "ts": ts,
                    "year": year,
                    "month": month,
                    "day": day,
                    "time": time,
                    "date": date
                }
            )

    def save_product(self, conn: Connection, 
                        product_id: str, 
                        restaurant_id: int, 
                        product_name: str,
                        product_price: int,
                        active_from: datetime,
                        active_to: datetime):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(product_id, restaurant_id, product_name, product_price, active_from, active_to)
                    VALUES (%(product_id)s, %(restaurant_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (product_id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "product_id": product_id,
                    "restaurant_id": restaurant_id,
                    "product_name": product_name,
                    "product_price": product_price,
                    "active_from": active_from,
                    "active_to": active_to
                }
            )

    def save_order(self, conn: Connection, 
                        user_id: int, 
                        restaurant_id: int, 
                        timestamp_id: int,
                        order_key: str,
                        order_status: str):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s)
                    ON CONFLICT (order_key) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        order_status = EXCLUDED.order_status;
                """,
                {
                    "user_id": user_id,
                    "restaurant_id": restaurant_id,
                    "timestamp_id": timestamp_id,
                    "order_key": order_key,
                    "order_status": order_status
                }
            )

    def save_fct_product_sales(self, conn: Connection, 
                        product_id: int, 
                        order_id: int, 
                        count: int,
                        price: float,
                        total_sum: float,
                        bonus_payment: float,
                        bonus_grant: float):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (product_id, order_id) DO UPDATE
                    SET
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                """,
                {
                    "product_id": product_id,
                    "order_id": order_id,
                    "count": count,
                    "price": price,
                    "total_sum": total_sum,
                    "bonus_payment": bonus_payment,
                    "bonus_grant": bonus_grant
                }
            )

    def save_courier(self, conn: Connection, courier_id: str, name: str):
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_couriers(courier_id, name)
                        VALUES (%(courier_id)s, %(name)s)
                        ON CONFLICT (courier_id) DO UPDATE
                        SET
                            name = EXCLUDED.name;
                    """,
                    {
                        "courier_id": courier_id,
                        "name": name
                    }
                )

    def save_fct_deliveries(self, conn: Connection, 
                        delivery_id: str, 
                        courier_id: int, 
                        order_ts: datetime,
                        order_id: int,
                        rate: int,
                        tip_sum: float):
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.fct_deliveries(delivery_id, courier_id, order_ts, order_id, rate, tip_sum)
                        VALUES (%(delivery_id)s, %(courier_id)s, %(order_ts)s, %(order_id)s, %(rate)s, %(tip_sum)s)
                        ON CONFLICT (delivery_id) DO UPDATE
                        SET
                            courier_id = EXCLUDED.courier_id,
                            order_ts = EXCLUDED.order_ts,
                            order_id = EXCLUDED.order_id,
                            rate = EXCLUDED.rate,
                            tip_sum = EXCLUDED.tip_sum;
                    """,
                    {
                        "delivery_id": delivery_id,
                        "courier_id": courier_id,
                        "order_ts": order_ts,
                        "order_id": order_id,
                        "rate": rate,
                        "tip_sum": tip_sum
                    }
                )
