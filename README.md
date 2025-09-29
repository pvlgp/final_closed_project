# Итоговый закрытый проект  
  
## 1. Список разработчиков
1) Гапченко Павел

## 2. Цель проекта
Разработать демо-версию аналитической системы для сетевого магазина "Пикча", которая позволит им выгоднее и эффективнее продавать товары.

## 3. Описание пайплайна
Для реализации демо-версии аналитической системы используется следующий пайплайн:
1) Скрипт `generator_picture.py` генерирует набор атрибутов магазинов, портрета покупателей, товаров и покупок в формате JSON.
 - Запуск скрипта производится командой `python3 generator_picture.py` для Mac OS и Linux, `python generator_picture.py` для Windows;
2) Скрипт `loader_nosql.py` загружает сгенерированные JSON в базу данных MongoDB.
 - Перед запуском необходимо инициализировать переменные окружения `CLIENT_MONGO` - url для подключения к MongoDB, `DB_NAME` - имя базы данных в MongoDB.
 - Запуск скрипта производится командой `python3 loader_nosql.py` для Mac OS и Linux, `python loader_nosql.py` для Windows;
3) Скрипт `producer_from_mongodb.py` подключается к базе данных MongoDB и загружает данные в Kafka. 
 - Перед запуском необходимо инициализировать переменные окружения `CLIENT_MONGO` - url для подключения к MongoDB, `DB_NAME` - имя базы данных в MongoDB, `KAFKA_BOOTSTRAP_SERVERS` - url для подключения к Kafka.
 - Запуск скрипта производится командой `python3 producer_from_mongodb.py` для Mac OS и Linux, `python producer_from_mongodb.py` для Windows;
4) Скрипт `consumer_in_clickhouse.py` подключается к Kafka и загружает данные в сырое хранилище Clickhouse (RAW).
 - Перед запуском необходимо инициализировать переменные окружения `KAFKA_BOOTSTRAP_SERVERS` - url для подключения к Kafka, `CLICKHOUSE_HOST` - хост для подключения к Clickhouse, `CLICKHOUSE_PORT` - порт для подключения к Clickhouse, `CLICKHOUSE_USER` - имя пользователя, `CLICKHOUSE_PASSWORD` - пароль, `CLICKHOUSE_DATABASE` - имя базы данных Clickhouse.
 - Запуск скрипта производится командой `python3 generator_picture.py` для Mac OS и Linux, `python generator_picture.py` для WindowsЗапуска скрипта производится командой `python3 consumer_in_clickhouse.py` для Mac OS и Linux, `python consumer_in_clickhouse.py` для Windows.

## 4. Промежуточный этап выполнения
### Данные успешно загружены в сырое хранилище (Clickhouse RAW)
![Снимок экрана 2025-09-29 в 15.53.37.png](%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%202025-09-29%20%D0%B2%2015.53.37.png)