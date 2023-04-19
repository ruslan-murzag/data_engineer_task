from pymongo import MongoClient
import random
from datetime import datetime, timedelta
import pprint
import pandas as pd

# Установил связь с бд
client = MongoClient('localhost', 27017)

db = client["dbtest"]
# Создал коллекцию Country
country_collection = db["Country"]
operator_collection = db['Operators']


def insert_data(name, code, collection):
    data = {
        'name': name,
        'code': code
    }
    collection.insert_one(data)


insert_data("Kazakhstan", "+7", country_collection)
insert_data("USA", "+1", country_collection)
insert_data("Uruguay", "+5", country_collection)
insert_data("Uzbekistan", "+9", country_collection)
insert_data("Vatican", "+3", country_collection)

insert_data('Beeline', '705', operator_collection)
insert_data('Kcell', '777', operator_collection)
insert_data('Tele2', '747', operator_collection)
insert_data('Altel', '743', operator_collection)
insert_data('Activ', '701', operator_collection)
insert_data('Megafon', '923', operator_collection)
insert_data('MTC', '978', operator_collection)


journal_collection = db['Journal']


# функция для получения данных с коллекции
def collect_find_one(collection, r_number, data):
    return collection.find_one(skip=random.randint(0, r_number-1))[data]


# функция для получения кода с коллекции
def collect_find_code(collection, code):
    return collection.find_one({'code': code})['name']


# Цикл для данных
for i in range(0,100):
    # Посчитал количество записей в коллеции Country
    len_countries = country_collection.count_documents({})
    # Посчитал количество записей в коллеции Operators
    len_operator = operator_collection.count_documents({})
    # Рандомная страна
    a_random_country = collect_find_one(country_collection, len_countries,  'code')

    # Рандомный оператор
    a_random_operator = collect_find_one(operator_collection, len_operator, 'code')
    # Рандомная страна
    b_random_country = collect_find_one(country_collection, len_countries, 'code')
    # Рандомная оператор
    b_random_operator = collect_find_one(operator_collection, len_operator, 'code')
    # Рандомные числа
    a_random_numbers = str(random.randint(1000000, 9999999))
    b_random_numbers = str(random.randint(1000000, 9999999))

    # Создал номер
    a_number = a_random_country+a_random_operator+a_random_numbers
    b_number = b_random_country+b_random_operator+b_random_numbers
    # Длительность
    duration = random.randint(1, 3600)
    # Начало
    start_time = datetime(2023, 4, 17) + timedelta(seconds=random.randint(78000, 120000))

    """Здесь я сразу взял код, Но если мне было бы дано просто строка
    то я использовал бы регулярные выражения или просто взял бы код по нумерации строки
    """
    a_operator = collect_find_code(operator_collection, a_random_operator)
    a_country = collect_find_code(country_collection, a_random_country)
    b_operator = collect_find_code(operator_collection, b_random_operator)
    b_country = collect_find_code(country_collection, b_random_country)

    call = {
        "a_number": a_number,
        "a_country": a_country,
        "a_operator": a_operator,
        "b_number": b_number,
        "b_country": b_country,
        "b_operator": b_operator,
        "datetime": start_time,
        "duration": duration
    }
    journal_collection.insert_one(call)
#
# Задача 7
data = journal_collection.find({})
data_journal = pd.DataFrame.from_records(data)
# by_country = data_journal.groupby('b_country').agg({"_id":'count',"duration":"sum"})
#
# Задача 8
april_18 = data_journal[data_journal["datetime"].dt.date == pd.to_datetime('2023-04-18').date()]
counter = april_18.groupby("a_number").agg({"datetime": "count"})
filtered = counter[counter["datetime"] >= 3]
# print(filtered)

# Задача 7,8 через pymongo

pipeline1 = [
    {'$group':{"_id": "$b_country", "count_country": {"$sum": 1}, "total_duration_country": {"$sum": "$duration"}}},
    {"$project":{"_id": 0, "b_country": "$_id", "count_country": 1, "total_duration_country": 1}}
]
result = list(journal_collection.aggregate(pipeline1))
# print(result)

date_query = {
    "datetime": {
        "$gte": datetime(2023, 4, 18),
        "$lt": datetime(2023, 4, 19)
    }
}
pipeline2 = [
    {"$match": date_query},
    {'$group': {"_id": "$a_number", "count": {"$sum": 1}}},
    {"$match": {"count": {"$gte": 3}}},
    {'$project': {'_id': 0, 'a_number': "$_id", "count": 1}}
]

result2 = list(journal_collection.aggregate(pipeline2))
