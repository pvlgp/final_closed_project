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
    "db": os.getenv("DB_NAME")
}

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
    pass
