from twitter_mongodb.twitterdb_instance import DbInstance


class UserDao:
    db_name = 'twitter'
    port = 27017
    collection = 'twitternetworks'

    def __init__(self):
        self.db = DbInstance(UserDao.port, UserDao.db_name).getDbInstance()



    def save_friends(self, user_id, friends):
        friends = list(friends)
        self.db[self.collection].update_one({'id': user_id}, {'$set': {'friends': friends}}, True)
        print(user_id)

    def get_friends(self, user_id):
        friends = []
        user_data = self.db[self.collection].find_one({'id': user_id}, {'_id': 0, 'friends': 1})
        if user_data is not None:
            for f in user_data['friends']:
                friends.append(f)

        return friends

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


    # print(UserDao().get_friends('120639382') )
