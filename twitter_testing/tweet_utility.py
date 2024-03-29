from persistance_mongo_layer.dao.clique_dao import get_order_descending_maximal_cliques
from persistance_mongo_layer.twitterdb_instance import DbInstance

collection = 'tweets'
db_name = 'twitter'
port = 27017
#port = 27018

def get_users_tweet(users_ids, db=None):
    try:
        users_ids = list(users_ids)
    except TypeError:
        print 'users ids has to be an iterable'
    #db = DbInstance(port, db_name).getDbInstance(new_client=new_client)
    if db == None:
        db= DbInstance().getDbInstance()
    pipeline = [
        {"$match":{"user":{"$in":users_ids} } },
        {"$unwind":"$hashtags"},
        {"$unwind":"$mentions"},
        {"$project":{"hashtag":"$hashtags.text", "mention":"$mentions.id_str", "mention_name":"$mentions.name", "user":1} },
        {"$group":{
            "_id":"$user",
            "hashtags":{"$push":"$hashtag"},
            "mentions":{"$push":"$mention"},
            "mentions_names": {"$push": "$mention_name"}
        } }
    ]

    tweets = db[collection].aggregate(pipeline)
    return list(tweets)

def get_users_tweet(users_ids, db=None):
    try:
        users_ids = list(users_ids)
    except TypeError:
        print 'users ids has to be an iterable'
    #db = DbInstance(port, db_name).getDbInstance(new_client=new_client)
    if db == None:
        db= DbInstance().getDbInstance()


    return db[collection].find({'user':{'$in':users_ids}})


def clean_tweets_collection():
    clqs = get_order_descending_maximal_cliques()
    users_per_clique = [set(clq['nodes']) for clq in clqs]
    ids = set.union(*users_per_clique)
    db = DbInstance(port, db_name).getDbInstance()
    db[collection].remove({"user":{"$nin": list(ids)} })


#print get_users_tweet(["67613844"])
#clean_tweets_collection()