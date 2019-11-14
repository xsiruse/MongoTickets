import csv
import re
from datetime import datetime
from pprint import pprint
from pymongo import MongoClient


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        # reader = csv.DictReader(csvfile)
        # # for row in reader:
        # #     add_artist(db, row['Исполнитель'])
        # #     add_place(db, row['Место'])
        # db.concert_coll.insert_many(reader)
        data = []
        reader = csv.reader(csvfile)
        next(reader, None)
        for line in reader:
            artist, price, place, date = line
            data.append({
                'Исполитель': artist,
                'Цена': int(price),
                'Место': place,
                'Дата': datetime.strptime(date, '%d.%m')
            })
        db.concert_coll.insert_many(data)


# def add_artist(db, artist):
#     coll_artist = db.artist
#     if coll_artist.find_one({'artist': artist}):
#         return None
#     res = coll_artist.insert_one({'artist': artist})
#     return print(res.inserted_id)
#
# def add_place(db, place):
#     coll_place = db.place
#     if coll_place.find_one({'place': place}):
#         return None
#     res = coll_place.insert_one({'place': place})
#     return print(res.inserted_id)


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    res = list(db.concert_coll.find().sort('Цена', 1))
    print('Отсортировано по цене по возрастанию:\n')
    pprint(res)


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    pass
    regex = re.compile(r'\.*%s' % name, re.IGNORECASE)
    artist = db.concert_coll.find({"Исполитель": {'$regex': regex}}).sort('Цена', 1)
    print('\nРезультат поиска:\n')
    pprint(list(artist))


def sort_by_date(db):
    res = list(db.concert_coll.find().sort('Дата', 1))
    print('\nОтсортировано по дате возрастанию:\n')
    pprint(res)


def main():
    client = MongoClient()
    db = client['concert']
    source = 'artists.csv'
    read_data(source, db)
    find_cheapest(db)
    find_by_name('th', db)
    sort_by_date(db)
    db.concert_coll.drop()


if __name__ == '__main__':
    main()
