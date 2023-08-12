import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.dds.dds_dag.pg_saver import PgSaver
from examples.dds.dds_dag.user_reader import UserReader
from examples.dds.dds_dag.user_loader import UserLoader
from examples.dds.dds_dag.restaurant_loader import RestaurantLoader
from examples.dds.dds_dag.restaurant_reader import RestaurantReader
from examples.dds.dds_dag.timestamp_loader import TimestampLoader
from examples.dds.dds_dag.timestamp_reader import TimestampReader
from examples.dds.dds_dag.product_loader import ProductLoader
from examples.dds.dds_dag.product_reader import ProductReader
from examples.dds.dds_dag.order_loader import OrderLoader
from examples.dds.dds_dag.order_reader import OrderReader
from examples.dds.dds_dag.fct_product_sales_loader import SalesLoader
from examples.dds.dds_dag.fct_product_sales_reader import SalesReader
from examples.dds.dds_dag.courier_loader import CourierLoader
from examples.dds.dds_dag.courier_reader import CourierReader
from examples.dds.dds_dag.fct_deliveries_loader import DeliveryLoader
from examples.dds.dds_dag.fct_deliveries_reader import DeliveryReader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    bonus_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task()
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        #mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = UserReader(dwh_pg_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = RestaurantReader(dwh_pg_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_timestamps():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = TimestampReader(dwh_pg_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = TimestampLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_products():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = ProductReader(dwh_pg_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = ProductLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = OrderReader(dwh_pg_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_sales():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = SalesReader(dwh_pg_connect, bonus_pg_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = SalesLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = CourierReader(dwh_pg_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CourierLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_deliveries():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = DeliveryReader(dwh_pg_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = DeliveryLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()
    


    user_loader = load_users()
    restaurant_loader = load_restaurants()
    timestamp_loader = load_timestamps()
    product_loader = load_products()
    order_loader = load_orders()
    sales_loader = load_sales()
    courier_loader = load_couriers()
    delivery_loader = load_deliveries()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    user_loader >> restaurant_loader >> timestamp_loader >> product_loader >> order_loader >> sales_loader >> courier_loader >> delivery_loader # type: ignore


dds_dag = sprint5_dds()  # noqa
