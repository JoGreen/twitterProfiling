from pymongo import MongoClient

class DbInstance:
    db = None #not good enough as final solution -> DEPRECATED ?
    dbs = {}

    def __init__(self, port, db_name):  #not usefuls using dbname here
        self.port = port
        self.db_name = db_name
        self.db = MongoClient('localhost', port)


    def getDbInstance(self): #deprecated
        return self.db[self.db_name]# self.find_db_instance(self.db_name)

    def find_db_instance(self, db_name):
        if not self.dbs.has_key(db_name):
            self.set_db_instance(self.port, db_name)
        return self.dbs[db_name]


    def set_db_instance(self, port, db_name):
        # type:(DbInstance, int, str)-> None
        self.port = port
        self.db_name = db_name
        self.dbs.update({db_name: MongoClient('localhost', port)[db_name]})
