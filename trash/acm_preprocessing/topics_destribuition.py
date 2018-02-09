from persistance_mongo_layer.twitterdb_instance import DbInstance
from operator import itemgetter


def topic_destribuition_on_paper():
    db= DbInstance().getDbInstance()
    topic_destrib = db['paper2topic'].aggregate([{"$group":{"_id":"$topic" ,"count":{"$sum": 1} } } ])

    ordered_topic_destrib = sorted(list(topic_destrib), key=itemgetter("count"), reverse= True)

    print ordered_topic_destrib