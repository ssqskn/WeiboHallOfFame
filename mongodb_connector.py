#coding:utf8
import pymongo
    
class MongodbConnector():
    """ Mongodb Server should be started in advance:
        sh: ./bin/mongod -dbpath data
    """
    def __init__(self, ip="127.0.0.1", port="27017"):
        self.ip = ip
        self.port = int(port)
        self.client = pymongo.MongoClient(self.ip, self.port)
    
    def get_client(self):
        return self.client
    
    def get_database(self, db_name):
        db = self.client[str(db_name)]
        return db

    def get_collection(self, db_name, collection_name):
        db = self.get_database(str(db_name))
        collection = db[str(collection_name)]
        return collection


if __name__ == '__main__':
    mongodb = MongodbConnector()
    collection = mongodb.get_collection('test','test')
    """ examples for fetching records"""
    for i in collection.find(): print i
    print collection.find_one()
    print collection.find()[0]
    """ counting """
    print collection.count()
    print collection.find({"id":1}).count()
    """ Indexing"""
    results = collection.create_index([('id', pymongo.ASCENDING)], unique=True)
    print list(collection.index_information())
    """ find by time """
    """d = datetime.datetime(2009, 11, 12, 12)
       for post in posts.find({"date": {"$lt": d}}).sort("author"):
           print post
    """
    
    