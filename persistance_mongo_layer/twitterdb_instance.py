from pymongo import MongoClient
#import db_connections_data
import pamir_connection_data as db_connections_data

class DbInstance:
    db = None #not good enough as final solution -> DEPRECATED ?
    dbs = {}

    def __init__(self, port=None, db_name=None):  #not usefuls using dbname here
        if port == None:
            self.port = db_connections_data.port
        else:
            self.port = port
        if db_name == None:
            self.db_name = db_connections_data.db_name
        else:
            self.db_name = db_name

        #self.db = MongoClient(db_connections_data.host, self.port)[self.db_name]
        self.db = MongoClient(db_connections_data.url)[self.db_name]

    def getDbInstance(self, new_client= False): #deprecated
        #return self.db[self.db_name]#
        if new_client:
            return MongoClient(db_connections_data.host, self.port)[self.db_name]
        #return self.find_db_instance(self.db_name)
        return self.db


    def find_db_instance(self, db_name):
        if not self.dbs.has_key(db_name):
            self.set_db_instance(self.port, db_name)
        return self.dbs[db_name]


    def set_db_instance(self, port, db_name):
        # type:(DbInstance, int, str)-> None
        self.port = port
        self.db_name = db_name
        self.dbs.update({db_name: MongoClient(db_connections_data.host, port)[db_name]})


#DbInstance().getDbInstance()['testx'].insert({'lollone':90})