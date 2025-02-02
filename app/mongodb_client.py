from pymongo import MongoClient

class MongoDBClient:
    def __init__(self, host="localhost", port=27017):
        self.host = host
        self.port = port
        self.client = MongoClient(host, port)
        # Modifiquem això perquè no sigui None
        self.database = self.client["MongoDB_"]
        self.collection = self.database["sensors"]

    def close(self):
        self.client.close()
    
    def ping(self):
        return self.client.db_name.command('ping')
    # Aquest métode sera el important, ens permetra rebre la instancia!
    def getDatabase(self, database):
        self.database = self.client[database]
        return self.database

    def getCollection(self, collection):
        self.collection = self.database[collection]
        return self.collection
    
    def clearDb(self,database):
        self.client.drop_database(database)

    # Métodes auxiliars 
    # l'usarem a createsensor
    def insertDocument(self, doc):
        return self.collection.insert_one(doc)
    
    # Per borrar, ho farem a través del nom
    def deleteDocument(self, name):
        return self.collection.delete_one({'name': name})
