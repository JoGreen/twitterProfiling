from twitter_mongodb.twitterdb_instance import DbInstance

class ProfileDao:
    db_name = 'twitter'
    port = 27017

    def __init__(self):
        self.db = DbInstance(ProfileDao.port, ProfileDao.db_name).getDbInstance()

    def getAllProfiles(self):
        all = self.db['user_infos'].find()
        return all

    def getSomeProfiles(self, someUsers):
        someProfiles = self.db['user_infos'].find({"user":{"$in":someUsers} })
        return someProfiles

