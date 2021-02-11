from pymongo import *


class Mongo:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client['SummerResearch']

    def getClient(self):
        return self.client

    def getDb(self):
        return self.db

