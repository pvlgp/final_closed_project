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

def get_data_from_mongodb(url: str, db_name: str, collect_name: str):
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
        client = MongoClient(url, serverSelectionTimeoutMS=5000)
        # проверка подключения
        conn_yes = client.admin.command("ping")
        if conn_yes:
            logger.info("Подключение к MongoDB установлено")
        else:
            raise ServerSelectionTimeoutError
        # получаем базу данных и коллекцию
        db = client[db_name]

        collect = db[collect_name]

        # получаем документы, исключаем поле '_id'
        documents = list(collect.find({}, {"_id": 0}))
        logger.info(f"Получено {len(documents)} документов в коллекции {collect_name}")
        return documents

    except ServerSelectionTimeoutError as SSTerr:
        logger.info(f"Ошибка подключения к MongoDB: {SSTerr}")
    except PyMongoError as mongo_err:
        logger.error(f"Ошибка при работе с MongoDB: {mongo_err}")
        return []
    except Exception as err:
        logger.error(f"Непредвиденная ошибка: {err}")
        return []
    finally:
        if "client" in locals():
            client.close()
            logger.info("Соединение с MongoDB закрыто")

documents = get_data_from_mongodb(os.getenv("CLIENT"), os.getenv("DB_NAME"), "products")

for d in documents:
    print(d)
