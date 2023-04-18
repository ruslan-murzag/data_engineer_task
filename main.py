from pymongo import MongoClient
import random
from datetime import datetime, timedelta
import pprint
import pandas as pd

#Установил связь с бд
client = MongoClient('localhost', 27017)

db = client["dbtest"]
# Создал коллекцию Country
country_collection = db["Country"]

# Создал данные для коллекции
data1 = {'name': "Kazakhstan", "code": "+7"}
data2 = {'name': "USA", "code": "+1"}
data3 = {'name': "Uruguay", "code": "+5"}
data4 = {'name': "Uzbekistan", "code": "+9"}
data5 = {'name': "Vatican", "code": "+3"}

# Добавил данные в коллекцию
datadb = country_collection.insert_many([data1, data2, data3, data4, data5])

# Тоже самое для Оператора
operator_collection = db['Operators']
o_data1 = {"name": "Beeline", "code": "705"}
o_data2 = {"name": "Kcell", "code": "777"}
o_data3 = {"name": "Tele2", "code": "747"}
o_data4 = {"name": "Altel", "code": "747"}
o_data5 = {"name": "Activ", "code": "701"}
o_data6 = {"name": "Megafon", "code": "923"}
o_data7 = {"name": "MTC", "code": "978"}

datadb1 = operator_collection.insert_many([o_data1, o_data2, o_data3, o_data4, o_data5, o_data6, o_data7])

journal_collection = db['Journal']

# Цикл для данных
for i in range(0,100):
    # Посчитал количество записей в коллеции Country
    len_countries = country_collection.count_documents({})
    # Посчитал количество записей в коллеции Operators
    len_operator = operator_collection.count_documents({})
    # Рандомная страна
    a_random_country = country_collection.find_one(skip=random.randint(0, len_countries-1))['code']
    # Рандомный оператор
    a_random_operator = operator_collection.find_one(skip=random.randint(0, len_operator-1))['code']
    # Рандомная страна
    b_random_country = country_collection.find_one(skip=random.randint(0, len_countries-1))['code']
    # Рандомная оператор
    b_random_operator = operator_collection.find_one(skip=random.randint(0, len_operator-1))['code']
    # Рандомные числа
    a_random_numbers = str(random.randint(1000000, 9999999))
    b_random_numbers = str(random.randint(1000000, 9999999))

    # Создал номер
    a_number = a_random_country+a_random_operator+a_random_numbers
    b_number = b_random_country+b_random_operator+b_random_numbers
    # Длительность
    duration = random.randint(1, 3600)
    # Начало
    start_time = datetime(2023, 4, 17) + timedelta(seconds=random.randint(50000, 106400))

    """Здесь я сразу взял код, Но если мне было бы дано просто строка
    то я использовал бы регулярные выражения или просто взял бы код по нумерации строки
    """
    a_operator = operator_collection.find_one({'code': a_random_operator})['name']
    a_country = country_collection.find_one({'code': a_random_country})['name']
    b_operator = operator_collection.find_one({'code': b_random_operator})['name']
    b_country = country_collection.find_one({'code': b_random_country})['name']

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

# Задача 7
data = journal_collection.find({})
data_journal = pd.DataFrame.from_records(data)
by_country = data_journal.groupby('b_country').agg({"_id":'count',"duration":"sum"})

# Задача 8
april_18 = data_journal[data_journal["datetime"].dt.date == pd.to_datetime('2023-04-18').date()]
counter = april_18.groupby("a_number").agg({"datetime": "count"})
filtered = counter[counter["datetime"] >= 3]