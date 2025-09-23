"""
Consumer для загрузки данных из Kafka в Clickhouse
"""
import json
import logging
import os

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from dotenv import load_dotenv
from clickhouse_driver import Client as ClickhouseClient

# проверяем существование директории log
# в случае отсутствия создаем директорию log
if not os.path.exists("log"):
    os.makedirs("log")

load_dotenv()
# настраиваем логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("log/consumer_from_kafka.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("consumer_from_kafka")

# получаем параметры с конфиденциальной информацией
param_conf = {
    "kafka_server": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "clickhouse_host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "clickhouse_port": os.getenv("CLICKHOUSE_PORT", "9000"),
    "clickhouse_user": os.getenv("CLICKHOUSE_USER", "default"),
    "clickhouse_pass": os.getenv("CLICKHOUSE_PASSWORD", ""),
    "clickhouse_db": os.getenv("CLICKHOUSE_DATABASE", "default")
}

# Проверяем переменные окружения
logger.info("=== ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ===")
for key, value in param_conf.items():
    status = "✅" if value else "❌"
    logger.info(f"{status} {key}: {value}")

class SendToClickhouseRaw:
    def __init__(self, clickhouse_client: ClickhouseClient, db_name: str = "raw_storage"):
        self.clickhouse_client = clickhouse_client
        self.db_name = db_name

    def create_db(self):
        """Создает базу данных если она не существует"""
        if self.clickhouse_client is None:
            logger.error("Клиент ClickHouse не инициализирован")
            return False

        try:
            # Сначала подключаемся к системной базе для создания новой БД
            self.clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
            logger.info(f"✅ База данных {self.db_name} создана или уже существует")

            # Теперь переключаемся на созданную базу
            self.clickhouse_client.execute(f"USE {self.db_name}")
            logger.info(f"✅ Переключились на базу данных {self.db_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при создании базы данных: {e}")
            return False

    def send_to_products(self, kafka_message: dict) -> None:
        """Отправляет данные в таблицу products"""
        if self.clickhouse_client is None:
            logger.error("Клиент ClickHouse не инициализирован")
            return

        try:
            # Убедимся что используем правильную базу данных
            self.clickhouse_client.execute(f"USE {self.db_name}")

            self.clickhouse_client.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.db_name}.products
                (
                    product_id String,
                    product_name String,
                    group_name String,
                    description String,
                    calories Float32,
                    protein Float32,
                    fat Float32,
                    carbohydrates Float32,
                    price Float32,
                    unit String,
                    origin_country String,
                    expiry_days UInt8,
                    is_organic Bool,
                    barcode String,
                    manufacturer_inn String,
                    manufacturer_name String,
                    manufacturer_country String,
                    manufacturer_website String
                )
                ENGINE = MergeTree()
                ORDER BY (product_id)
                """
            )

            self.clickhouse_client.execute(
                f"""
                INSERT INTO {self.db_name}.products (
                    product_id, product_name, group_name, description, calories, protein, fat, 
                    carbohydrates, price, unit, origin_country, expiry_days, is_organic, barcode,
                    manufacturer_inn, manufacturer_name, manufacturer_country, manufacturer_website
                ) VALUES
                """,
                [(
                    kafka_message["id"],
                    kafka_message["name"],
                    kafka_message["group"],
                    kafka_message["description"],
                    kafka_message["kbju"]["calories"],
                    kafka_message["kbju"]["protein"],
                    kafka_message["kbju"]["fat"],
                    kafka_message["kbju"]["carbohydrates"],
                    kafka_message["price"],
                    kafka_message["unit"],
                    kafka_message["origin_country"],
                    kafka_message["expiry_days"],
                    kafka_message["is_organic"],
                    kafka_message["barcode"],
                    kafka_message["manufacturer"]["inn"],
                    kafka_message["manufacturer"]["name"],
                    kafka_message["manufacturer"]["country"],
                    kafka_message["manufacturer"]["website"]
                )]
            )
            logger.info("✅ Данные успешно отправлены в таблицу products")
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке данных в products: {e}")

    def send_to_customers(self, kafka_message: dict) -> None:
        """Отправляет данные в таблицу customers"""
        if self.clickhouse_client is None:
            logger.error("Клиент ClickHouse не инициализирован")
            return

        try:
            self.clickhouse_client.execute(f"USE {self.db_name}")

            self.clickhouse_client.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.db_name}.customers(
                    customer_id String,
                    first_name String,
                    last_name String,
                    email String,
                    phone String,
                    birth_date String,
                    gender String,
                    registration_date String,
                    is_loyalty_member Bool,
                    loyalty_card_number String,
                    purchase_location_country String,
                    purchase_location_city String,
                    purchase_location_street String,
                    purchase_location_house String,
                    postal_code String,
                    coordinates_latitude Float32,
                    coordinates_longitude Float32
                )
                ENGINE = MergeTree()
                ORDER BY (customer_id)
                """
            )

            # Безопасное извлечение purchase_location
            purchase_location = kafka_message.get("purchase_location", {})
            if "coordinates" in purchase_location:
                latitude = purchase_location["coordinates"].get("latitude", 0.0)
                longitude = purchase_location["coordinates"].get("longitude", 0.0)
            else:
                latitude = 0.0
                longitude = 0.0
                logger.warning("Поле coordinates в purchase_location отсутствует")

            self.clickhouse_client.execute(
                f"""
                INSERT INTO {self.db_name}.customers (
                    customer_id, first_name, last_name, email, phone, birth_date, gender,
                    registration_date, is_loyalty_member, loyalty_card_number,
                    purchase_location_country, purchase_location_city, purchase_location_street,
                    purchase_location_house, postal_code, coordinates_latitude, coordinates_longitude
                ) VALUES
                """,
                [(
                    kafka_message.get("customer_id", ""),
                    kafka_message.get("first_name", ""),
                    kafka_message.get("last_name", ""),
                    kafka_message.get("email", ""),
                    kafka_message.get("phone", ""),
                    kafka_message.get("birth_date", ""),
                    kafka_message.get("gender", ""),
                    kafka_message.get("registration_date", ""),
                    kafka_message.get("is_loyalty_member", False),
                    kafka_message.get("loyalty_card_number", ""),
                    purchase_location.get("country", ""),
                    purchase_location.get("city", ""),
                    purchase_location.get("street", ""),
                    purchase_location.get("house", ""),
                    purchase_location.get("postal_code", ""),
                    latitude,
                    longitude
                )]
            )
            logger.info("✅ Данные успешно отправлены в таблицу customers")
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке данных в customers: {e}")
            logger.debug(f"Структура сообщения customers: {json.dumps(kafka_message, indent=2)}")

    def send_to_purchases(self, kafka_message: dict) -> None:
        """Отправляет данные в таблицу purchases"""
        if self.clickhouse_client is None:
            logger.error("Клиент ClickHouse не инициализирован")
            return

        try:
            self.clickhouse_client.execute(f"USE {self.db_name}")

            self.clickhouse_client.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.db_name}.purchases (
                    purchase_id String,
                    customer_id String,
                    store_id String,
                    product_id String,
                    quantity UInt32,
                    total_amount Float32,
                    payment_method String,
                    is_delivery Bool,
                    delivery_country String,
                    delivery_city String,
                    delivery_street String,
                    delivery_house String,
                    delivery_apartment String,
                    postal_code String,
                    purchase_datetime String
                )
                ENGINE = MergeTree()
                ORDER BY (purchase_id)
                """
            )

            # Обрабатываем все items из покупки
            for item in kafka_message["items"]:
                self.clickhouse_client.execute(
                    f"""
                    INSERT INTO {self.db_name}.purchases (
                        purchase_id, customer_id, store_id, product_id, quantity, total_amount,
                        payment_method, is_delivery, delivery_country, delivery_city, delivery_street,
                        delivery_house, delivery_apartment, postal_code, purchase_datetime
                    ) VALUES
                    """,
                    [(
                        kafka_message["purchase_id"],
                        kafka_message["customer"]["customer_id"],
                        kafka_message["store"]["store_id"],
                        item["product_id"],
                        item["quantity"],
                        kafka_message["total_amount"],
                        kafka_message["payment_method"],
                        kafka_message["is_delivery"],
                        kafka_message["delivery_address"]["country"],
                        kafka_message["delivery_address"]["city"],
                        kafka_message["delivery_address"]["street"],
                        kafka_message["delivery_address"]["house"],
                        kafka_message["delivery_address"]["apartment"],
                        kafka_message["delivery_address"]["postal_code"],
                        kafka_message["purchase_datetime"]
                    )]
                )
            logger.info(f"✅ Данные успешно отправлены в таблицу purchases ({len(kafka_message['items'])} товаров)")
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке данных в purchases: {e}")

    def send_to_stores(self, kafka_message: dict) -> None:
        """Отправляет данные в таблицу stores"""
        if self.clickhouse_client is None:
            logger.error("Клиент ClickHouse не инициализирован")
            return

        try:
            self.clickhouse_client.execute(f"USE {self.db_name}")

            self.clickhouse_client.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.db_name}.stores (
                    store_id String,
                    store_name String,
                    store_network String,
                    store_type_description String,
                    type String,
                    manager_name String,
                    manager_phone String,
                    manager_email String,
                    location_country String,
                    location_city String,
                    location_street String,
                    location_house String,
                    postal_code String,
                    coordinates_latitude Float32,
                    coordinates_longitude Float32,
                    opening_hours String,
                    opening_hours_sat String,
                    opening_hours_sun String,
                    accepts_online_orders Bool,
                    delivery_available Bool,
                    warehouse_connected Bool,
                    last_inventory_date String
                )
                ENGINE = MergeTree()
                ORDER BY (store_id)
                """
            )

            # Безопасное извлечение coordinates из location
            location = kafka_message.get("location", {})
            if "coordinates" in location:
                latitude = location["coordinates"].get("latitude", 0.0)
                longitude = location["coordinates"].get("longitude", 0.0)
            else:
                latitude = 0.0
                longitude = 0.0
                logger.warning("Поле coordinates в location отсутствует")

            # Безопасное извлечение opening_hours
            opening_hours = kafka_message.get("opening_hours", {})
            opening_hours_str = opening_hours.get("opening_hours", "")
            opening_hours_sat = opening_hours.get("sat", "")
            opening_hours_sun = opening_hours.get("sun", "")

            # Безопасное извлечение manager
            manager = kafka_message.get("manager", {})
            manager_name = manager.get("name", "")
            manager_phone = manager.get("phone", "")
            manager_email = manager.get("email", "")

            self.clickhouse_client.execute(
                f"""
                INSERT INTO {self.db_name}.stores (
                    store_id, store_name, store_network, store_type_description, type,
                    manager_name, manager_phone, manager_email, location_country, location_city,
                    location_street, location_house, postal_code, coordinates_latitude,
                    coordinates_longitude, opening_hours, opening_hours_sat, opening_hours_sun,
                    accepts_online_orders, delivery_available, warehouse_connected, last_inventory_date
                ) VALUES
                """,
                [(
                    kafka_message.get("store_id", ""),
                    kafka_message.get("store_name", ""),
                    kafka_message.get("store_network", ""),
                    kafka_message.get("store_type_description", ""),
                    kafka_message.get("type", ""),
                    manager_name,
                    manager_phone,
                    manager_email,
                    location.get("country", ""),
                    location.get("city", ""),
                    location.get("street", ""),
                    location.get("house", ""),
                    location.get("postal_code", ""),
                    latitude,
                    longitude,
                    opening_hours_str,
                    opening_hours_sat,
                    opening_hours_sun,
                    kafka_message.get("accepts_online_orders", False),
                    kafka_message.get("delivery_available", False),
                    kafka_message.get("warehouse_connected", False),
                    kafka_message.get("last_inventory_date", "")
                )]
            )
            logger.info("✅ Данные успешно отправлены в таблицу stores")
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке данных в stores: {e}")
            # Добавим отладочную информацию
            logger.debug(f"Структура сообщения stores: {json.dumps(kafka_message, indent=2)}")


def get_list_topics(bootstrap_server: str, client_id: str = "topic_lister") -> list:
    """
    Функция получает список топиков в kafka
    """
    admin_client = None

    try:
        logger.info("Попытка подключения к Kafka")
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_server,
            client_id=client_id
        )

        logger.info("Попытка получения списка топиков Kafka")
        topics: list = admin_client.list_topics()
        if topics:
            logger.info(f"✅ Получено {len(topics)} топиков")
        else:
            logger.warning("Список пуст")
        return topics
    except Exception as list_top_err:
        logger.error(f"❌ Ошибка при загрузке списка топиков: {list_top_err}")
        return []
    finally:
        if admin_client:
            admin_client.close()

def create_clickhouse_client(
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 9000,
        database: str = "default"
    ) -> ClickhouseClient | None:
    """
    Функция создает и возвращает клиент Clickhouse
    """
    try:
        logger.info(f"🔗 Попытка подключения к Clickhouse: {host}:{port}")
        logger.info(f"📋 Параметры: user={user}, database={database}")

        # Сначала подключаемся к существующей базе (default)
        click_client = ClickhouseClient(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database="default"
        )

        # Проверяем подключение
        result = click_client.execute("SELECT 1")
        logger.info(f"✅ Подключение к Clickhouse установлено. Тестовый запрос: {result}")
        return click_client

    except Exception as click_err:
        logger.error(f"❌ Ошибка подключения к Clickhouse: {click_err}")
        return None

if __name__ == "__main__":
    # Проверяем наличие .env файла
    if not os.path.exists('.env'):
        logger.error("❌ Файл .env не найден!")

    # Получаем список топиков
    logger.info("=== ПОЛУЧЕНИЕ СПИСКА ТОПИКОВ KAFKA ===")
    list_topics = get_list_topics(bootstrap_server=param_conf["kafka_server"])

    if not list_topics:
        logger.error("❌ Не удалось получить список топиков Kafka")
        exit(1)

    # Создаем клиент ClickHouse - подключаемся к default базе
    logger.info("=== ПОДКЛЮЧЕНИЕ К CLICKHOUSE ===")
    client = create_clickhouse_client(
        user=param_conf["clickhouse_user"],
        password=param_conf["clickhouse_pass"],
        host=param_conf["clickhouse_host"],
        port=int(param_conf["clickhouse_port"]),
        database="default"
    )

    if client is None:
        logger.error("❌ Не удалось подключиться к ClickHouse")
        logger.error("💡 Проверьте:")
        logger.error("1. Запущен ли ClickHouse: sudo systemctl status clickhouse-server")
        logger.error("2. Доступен ли порт 9000: netstat -ln | grep 9000")
        logger.error("3. Правильные ли логин/пароль в .env файле")
        exit(1)

    # Создаем экземпляр класса для отправки данных
    send_click = SendToClickhouseRaw(client, db_name="raw_storage")

    # Создаем базу данных
    logger.info("=== СОЗДАНИЕ БАЗЫ ДАННЫХ ===")
    if not send_click.create_db():
        logger.error("❌ Не удалось создать базу данных")
        exit(1)

    # Обрабатываем сообщения из топиков
    logger.info("=== НАЧИНАЕМ ОБРАБОТКУ СООБЩЕНИЙ ===")

    # Фильтруем только нужные топики
    target_topics = [
        "mongodb_product_store_products",
        "mongodb_product_store_customers",
        "mongodb_product_store_purchases",
        "mongodb_product_store_stores"
    ]

    for topic in target_topics:
        if topic not in list_topics:
            logger.warning(f"⚠️ Топик {topic} не найден в Kafka, пропускаем")
            continue

        try:
            logger.info(f"📭 Обрабатываем топик: {topic}")

            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=param_conf["kafka_server"],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("UTF-8")),
                consumer_timeout_ms=10000
            )

            message_count = 0
            for message in consumer:
                message_value = message.value
                message_count += 1
                logger.info(f"📨 Получено сообщение #{message_count} из топика {topic}")

                # Обрабатываем сообщение в зависимости от топика
                if topic == "mongodb_product_store_products":
                    send_click.send_to_products(message_value)
                elif topic == "mongodb_product_store_customers":
                    send_click.send_to_customers(message_value)
                elif topic == "mongodb_product_store_purchases":
                    send_click.send_to_purchases(message_value)
                elif topic == "mongodb_product_store_stores":
                    send_click.send_to_stores(message_value)

            logger.info(f"✅ Обработано {message_count} сообщений из топика {topic}")
            consumer.close()

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке топика {topic}: {e}")
            continue

    logger.info("🎉 Обработка всех сообщений завершена!")