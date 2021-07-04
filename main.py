import pymongo
from pymongo import MongoClient
from pymongo.database import Database, Collection


def crea_db():
    # eseguo la connessione
    client = MongoClient('mongodb://admin:admin@localhost:27017/?compressors=zlib&gssapiServiceName=mongodb&ssl=false')

    # creo un nuovo database
    db = client.testdb

    # creo una nuova collection
    persone_collection = db.persone

    # creo gli indici sulla collezione
    persone_collection.create_index([('nome', pymongo.ASCENDING)])
    persone_collection.create_index([('cognome', pymongo.ASCENDING)])
    persone_collection.create_index([('computer', pymongo.ASCENDING)])

    # inserisco un documento
    p1 = {'nome': 'Mario',
          'cognome': 'Rossi',
          'eta': 30,
          'computer': ['Asus', 'Apple']}
    persone_collection.insert_one(p1)

    p2 = {'nome': 'Giuseppe',
          'cognome': 'Verdi',
          'eta': 45,
          'computer': ['Apple']}
    persone_collection.insert_one(p2)


def query():
    client = MongoClient('mongodb://admin:admin@localhost:27017/?compressors=zlib&gssapiServiceName=mongodb&ssl=false')
    db: Database = client.testdb
    persone_collection: Collection = db.persone
    p = persone_collection.find_one()
    print(p)

    print('***')
    persone_apple = persone_collection.find({'computer': 'Apple'})
    for p in persone_apple:
        print(p)

    print('***')
    persone_collection.update_one({'nome': 'Giuseppe'}, {'$set': {'eta': 50}})
    p = persone_collection.find_one({'nome': 'Giuseppe'})
    print(p)

    print('***')
    p = persone_collection.find_one({'nome': {'$gt': 'Giuseppe'}})
    print(p)

    persone_collection.map_reduce()


def main():
    # crea_db()
    query()


if __name__ == '__main__':
    main()
