import time

from twitter_api.user_info import UserInfo
from persistance_mongo_layer.dao.clique_dao import get_maximal_cliques
from persistance_mongo_layer.twitterdb_instance import DbInstance


class UserDao:
    #db_name = 'twitter'
    #port = 27017
    collection = 'twitternetworks'

    def __init__(self, db= None):
        if db == None:
            #self.db = DbInstance(UserDao.port, UserDao.db_name).getDbInstance()
            self.db = DbInstance().getDbInstance()
        else:
            self.db = db



    def save_friends(self, user_id, friends):
        friends = list(friends)
        self.db[self.collection].update_one({'id': user_id}, {'$set': {'friends': friends}}, True)
        print(user_id)

    def get_friends(self, user_id, db = None):
        if db != None:
            self.db = db
        friends = []
        user_data = self.db[self.collection].find_one({'id': user_id}, {'_id': 0, 'friends': 1})
        if user_data is not None:
            return user_data['friends']
        return friends

    def get_friends_of(self, ids):
        try:
            ids=list(ids)
        except TypeError:
            print 'type error in get_friends_of'
        friends_data = self.db[self.collection].find({"id":{"$in":ids}}, {'_id': 0, "id":1, 'friends': 1})
        return friends_data


    def get_followers(self, user_id):
        pass

    def get_friends_count(self, users_ids):
        users_friends_count = []
        users_friends_count_cursor = self.db[self.collection].find({'id': {'$in': users_ids}},
                                                                   {'_id': 0, 'friends_count': 1, 'friends': 1})
        print (users_friends_count_cursor.count(), 'users retrieved')
        for doc in users_friends_count_cursor:
            # print(doc['friends_count'])
            try:
                users_friends_count.append(doc['friends_count'])
            except KeyError:
                users_friends_count.append(len(doc['friends']))

        return users_friends_count


    def get_users_with_lt_friends(self, friends_count):
        users = self.db[self.collection].find({'friends_count': {'$lt' : friends_count}}, {'_id': 0, 'id': 1} )
        #print(users.count())
        return users


    def retrieve_users_friend(self):
        clqs = get_maximal_cliques()
        users_per_clique = [set(clq['nodes']) for clq in clqs]
        ids = set.union(*users_per_clique)
        ids_owned = UserDao().db[self.collection].find({}).distinct('id')
        ids_to_retrieve = ids.difference(ids_owned)
        print 'need to retrieve', len(ids_to_retrieve),'users friendship'
        ui = UserInfo()
        #docs = {}
        for id in ids_to_retrieve:
            friends = ui.get_friends(id)
            self.db[self.collection].insert_one({
                'id': id,
                'friends_count': len(friends),
                'friends': friends})
            time.sleep(60)
#print(UserDao().get_friends('120639382') )


#create_dataset(37000)
#UserDao().retrieve_users_friend()