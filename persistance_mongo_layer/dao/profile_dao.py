from persistance_mongo_layer.twitterdb_instance import DbInstance
#from twitter_clique_utilities.clique_dao import get_maximal_cliques, create_dataset
import operator


class ProfileDao:
    db_name = 'twitter'
    port = 27017
    profile_cache = {}

    def __init__(self):
        #self.db = DbInstance(ProfileDao.port, ProfileDao.db_name).getDbInstance()
        self.db = DbInstance().getDbInstance()

    def getAllProfiles(self):
        all = self.db['user_infos'].find()
        return all

    def getSomeProfiles(self, someUsers, db=None):
        #if all(self.profile_cache.has_key(u) for u in someUsers ):
        if db == None:
            db = self.db
        try:
            someUsers = list(someUsers)
            some_profiles = []
            for u in someUsers:
                some_profiles.append(self.profile_cache[u])

        except KeyError:
        #else:
            some_profiles = db['user_infos'].find({"user":{"$in":someUsers} })
        return some_profiles

    def get_users_without_interests(self):
        users_id = self.db['user_infos'].find({"info.interests.all":{"$exists": False}},{'user':1, '_id':0})
        return users_id

    def get_all_useful_profiles(self, skip = 0, limit = 0, cache= False):
        all= self.db['user_infos'].find({"info.interests.all":{"$exists": True}},{"info.platform":0, "info.valid":0, "info.type":0, "_id":0, "__v":0, "info.gender":0} )
        all.skip(skip)
        #all.limit(limit)
        self.state = 0
        if cache:
            map(self.initialize_profile_cache, all)
            print len(self.profile_cache.keys() )
        return all

    def initialize_profile_cache(self, p):
        self.state = self.state + 1
        print self.state
        ProfileDao.profile_cache[p["user"]] = p

    def load_profile_cache(self):
        self.get_all_useful_profiles(0,5000)
        self.get_all_useful_profiles(5000, 10000)
        self.get_all_useful_profiles(10000, 15000)

    def interests_destribuition(self, users = None):
    # type:(list)->dict
        if users == None:
            docs = self.db['user_infos'].find({"info.interests.all":{"$exists": True}},{"info.platform":0, "info.valid":0, "info.type":0, "_id":0, "__v":0, "info.gender":0} )
        else:
            docs = self.db['user_infos'].find({
                "user":{"$in":users}, "info.interests.all": {"$exists": True} },
                {"info.platform": 0, "info.valid": 0, "info.type": 0, "_id": 0, "__v": 0, "info.gender": 0}
            )

        counting_map = {}
        for u in docs:
            interests = u['info']['interests']['all']
            for interest in interests.keys():
                try:
                    counting_map[interests[interest]['name']] = counting_map[interests[interest]['name']] + 1
                except:
                    counting_map[interests[interest]['name']] = 1

        counting_map = sorted(counting_map.items(), key=operator.itemgetter(1))
        return counting_map



# print ProfileDao().interests_destribuition()

    def get_all_useful_user_ids(self, skip=0, limit=0):
        all = self.db['user_infos'].find({"info.interests.all": {"$exists": True}}, {'_id':0, 'user':1}).distinct('user')
        return all

    def get_new_client(self):
        return DbInstance(ProfileDao.port, ProfileDao.db_name).getDbInstance(new_client=True)


#print ProfileDao().get_users_without_interests().count()