"""
Producer для загрузки информации в Kafka из MongoDB
"""
import json
import logging
import os

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from dotenv import load_dotenv
from kafka import KafkaProducer

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
        logging.FileHandler("log/producer_from_mongodb.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("producer_from_mongodb")

# получаем параметры с конфиденциальной информацией
param_conf = {
    "client": os.getenv("CLIENT_MONGO"),
    "db": os.getenv("DB_NAME"),
    "Kafka_server": os.getenv("KAFKA_BOOTSTRAP_SERVERS")
}

def get_list_collections(url: str, db_name) -> list:
    """
    Функция получает список коллекций в указанной базе данных
    :param url: URL для подключения к серверу MongoDB
    :param db_name: Имя базы данных, к которой нужно подключиться
    :return: объект list со списком коллекций
    """
    try:
        logger.info(f"Попытка подключения к БД {db_name}")
        # подключаемся к базе данных
        with MongoClient(url, serverSelectionTimeoutMS=5000) as client:
            db = client[db_name]
            logger.info(f"Попытка получения списка коллекций в БД {db_name}")
            # получаем список коллекций
            collections_names = db.list_collection_names()
            logger.info("Список коллекций успешно получен")
            return collections_names
    except Exception as mon_err:
        logger.error(f"Ошибка при работе с MongoDB: {mon_err}")
        # при ошибке возвращаем пустой список
        return []

def get_data_from_mongodb(url: str, db_name: str, collect_name: str) -> list:
    """
    Функция выполняет подключение к MongoDB и получает документы из указанной коллекции
    :param url: URL для подключения к серверу MongoDB
    :param db_name: Имя базы данных, к которой нужно подключиться
    :param collect_name: имя коллекции для получения документов
    :return: список с документами из указанной коллекции
    """
    try:
        # подключаемся к MongoDB
        logger.info("Попытка подключения к MongoDB")
        with MongoClient(url, serverSelectionTimeoutMS=5000) as client:
            # проверка подключения
            client.admin.command("ping")
            logger.info("Подключение к MongoDB установлено")
            # получаем базу данных и коллекцию
            db = client[db_name]
            collect = db[collect_name]
            # получаем документы, исключаем поле '_id'
            documents = list(collect.find({}, {"_id": 0}))
            logger.info(f"Получено {len(documents)} документов в коллекции {collect_name}")
            return documents

    except ServerSelectionTimeoutError as SSTerr:
        logger.error(f"Ошибка подключения к MongoDB: {SSTerr}")
        return []
    except PyMongoError as mongo_err:
        logger.error(f"Ошибка при работе с MongoDB: {mongo_err}")
        return []
    except Exception as err:
        logger.error(f"Непредвиденная ошибка: {err}")
        return []

def send_to_kafka(producer: KafkaProducer, topic: str, data: list) -> None:
    """
    Функция выполняет отправку сообщений в Kafka
    :param producer: Конфигурация Kafka Producer
    :param topic: Название топика
    :param data: Данные для передачи в Kafka
    :return: None
    """
    # Проверяем наличие данных для отправки
    if not data:
        logger.warning("Нет данных для отправки")
        return

    try:
        success_count = 0 # подсчет количества успешных отправлений
        failed_count = 0 # подсчет ошибок при отправлении
        # Проходим по списку документов полученных из MongoDB
        for document in data:
            try:
                logger.info("Попытка отправки сообщения в Kafka")
                # отправляем сообщение в топик
                producer.send(
                    topic=topic,
                    value=json.dumps(document).encode("UTF-8")
                )
                # при успехе увеличиваем переменную success_count на единицу
                success_count += 1
                logger.info(f"Сообщение успешно отправлено в топик {topic}")
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения в топик {topic}: {e}")
                # при ошибке увеличиваем переменную failed_count на единицу
                failed_count += 1
        producer.flush()
        logger.info(f"Успешно отправлено {success_count}/{len(data)} сообщений в топик {topic}. Количество ошибок {failed_count}.")
    except Exception as err:
        logger.error(f"Ошибка при отправке в Kafka {err}")

def main() -> None:
    """
    Основная функция выполняющая ETL процесс отправки данных в Kafka.
    Extract - из MongoDB, Load - в Kafka.
    :return: None
    """
    try:
        logger.info("Запуск Producer для загрузки данных из MongoDB в Kafka")
        # получаем список коллекций в указанной БД
        list_collections = get_list_collections(param_conf["client"], param_conf["db"])
        logger.info(f"Найдены коллекции: {", ".join(list_collections)}")
        # создаем Kafka Producer
        logger.info("Создание Kafka Producer")
        producer = KafkaProducer(
            bootstrap_servers=param_conf["Kafka_server"],
            acks="all",
            retries=3
        )
        # проходим по списку с коллекциями
        for collection_name in list_collections:
            logger.info(f"Обработка коллекции: {collection_name}")
            # получаем данные из коллекции
            data = get_data_from_mongodb(
                url=param_conf["client"],
                db_name=param_conf["db"],
                collect_name=collection_name
            )
            # определяем имя топика
            kafka_topik = f"mongodb_{param_conf["db"]}_{collection_name}"
            # отправляем данные в топик
            logger.info(f"Отправка {len(data)} документов в топик {kafka_topik}")
            send_to_kafka(
                producer=producer,
                topic=kafka_topik,
                data=data
            )
            logger.info(f"Коллекция {collection_name} обработана")
        producer.close()
        logger.info("ETL процесс завершен")
    except Exception as main_err:
        logger.error(f"Ошибка в функции main: {main_err}")
    finally:
        logger.info("Завершение работы Producer")

if __name__ == "__main__":
    main()
