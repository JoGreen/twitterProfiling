from twitter_mongodb.twitterdb_instance import DbInstance


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
        db= DbInstance(port, db_name).getDbInstance()
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
    #print pipeline
    tweets = db[collection].aggregate(pipeline)
    return list(tweets)




