"""
Загрузчик файлов JSON в noSQL базу данных (MondoDB)
"""

import json
import logging
import os

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

load_dotenv()
# настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("log/loader_nosql.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("loader_nosql")

def load_json_in_mongodb(directory, collection_name) -> None:
    """
    Загрузка json файлов в MongoDB
    :param directory: путь к json файлам
    :param collection_name: коллекция в базе данных MongoDB
    :return: None
    """
    try:
        logger.info(f"Попытка получения списка файлов в директории {directory}")
        # Получаем список файлов в директории
        with os.scandir(directory) as files:
            for file in files:
                # Проходим по списку
                # Проверяем, что это файл
                if file.is_file():
                    logger.info(f"Попытка чтения JSON файла {file.name}")
                    # Читаем файл и отправляем содержимое в коллекцию MongoDB
                    with open(file.path, "r", encoding="UTF-8") as f:
                        data = json.load(f)
                        logger.info("Попытка записи содержимого файла в базу данных MongoDB")
                        collection_name.insert_one(data)
    except PyMongoError as e:
        logger.error(f"Ошибка при загрузке данных в базу данных: {e}")

try:
    # создание подключения к MongoDB
    logger.info("Попытка подключения к MongoDB")
    client = MongoClient(os.getenv("CLIENT"))
    # Создание базы данных
    logger.info("Попытка обращения/создания к базе данных")
    data_base = client["product_store"]

    # создание коллекций
    logger.info("Попытка обращения/создания к коллекции")
    customers = data_base["customers"]
    products = data_base["products"]
    purchases = data_base["purchases"]
    stores = data_base["stores"]
    # Для каждой коллекции делаем отдельный вызов функции
    load_json_in_mongodb("data/customers/", customers)
    load_json_in_mongodb("data/products/", products)
    load_json_in_mongodb("data/purchases/", purchases)
    load_json_in_mongodb("data/stores/", stores)

except PyMongoError as mongo_err:
    logger.error(f"Ошибка при работе с MongoDB: {mongo_err}")
except Exception as err:
    logger.error(f"Ошибка: {err}")
