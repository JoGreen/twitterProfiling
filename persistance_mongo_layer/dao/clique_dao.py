import itertools
import sys
import time

from bson.objectid import ObjectId
from pymongo import DESCENDING
from pymongo.cursor import Cursor
from pymongo.errors import InvalidId
from persistance_mongo_layer.dao.profile_dao import ProfileDao

from persistance_mongo_layer.dao.community_dao import drop
from persistance_mongo_layer.twitterdb_instance import DbInstance

collection = 'comms'
twitter_gt = 7
dblp_gt = 1

def get_maximal_cliques(grower_than = 6):
    # type: (int) -> Cursor
    #db = DbInstance(27017, 'twitter').getDbInstance()
    db = DbInstance().getDbInstance()
    cliques = db[collection].find({"count": {"$gt": grower_than} },
                                  no_cursor_timeout=True).sort([("count", DESCENDING)])

    return cliques


def get_limit_maximal_cliques(limit, grower_than = 6):
    # type: (int, int) -> Cursor
    clq = get_maximal_cliques(grower_than)
    clq.limit(limit)
    print(clq.count(), 'total cliques')
    return clq

def get_limit_maximal_cliques_on_valid_users(limit, gt=7):
    clqs = get_maximal_cliques_on_valid_users(gt= gt)
    clqs.limit(limit)
    return clqs

def get_maximal_cliques_on_valid_users(gt= 7): # doesn t work
    us2avoid = ProfileDao().get_users_without_interests()
    print 'users with no interests: ', us2avoid.count()
    us = [u['user'] for u in us2avoid]
    #for i in range(0, 1929): us.pop()
    #print len(us)

    #db = DbInstance(27017, 'twitter').getDbInstance()
    db = DbInstance().getDbInstance()
    cliques = db['cliques2analyze'].find({'nodes':{'$nin': us} ,
                                          'count':{'$gt':gt} }, no_cursor_timeout= True)
    print 'cliques retrieved: ', cliques.count()
    return cliques

def get_maximal_clique_on_specific_users_friendship_count(friendship_count, gt = 7):
    from persistance_mongo_layer.dao.user_dao import UserDao
    us = UserDao().get_users_with_lt_friends(friendship_count)
    users = [u['id'] for u in us]

    #db = DbInstance(27017, 'twitter').getDbInstance()
    db = DbInstance().getDbInstance()

    cliques = db[collection].find({'nodes': {'$in': users}, 'count':{'$gt':gt}}, no_cursor_timeout=True)

    return cliques

def get_limit_maximal_clique_on_specific_users_friendship_count(friendship_count, limit):
    # type:(int, int)->Cursor
    cliques = get_maximal_clique_on_specific_users_friendship_count(friendship_count)
    cliques.limit(limit)
    print(cliques.count())
    return cliques


def get_similar_cliques_on_nodes(clique_id, clique_nodes, k=1):  # check if id needs casting to ObjIdMongo
    #type: (str, list[str], int)->Cursor
    #db = DbInstance(27017, 'twitter').getDbInstance()
    db = DbInstance().getDbInstance()
    # verifing input
    try:
        clique_nodes = list(clique_nodes)
        clique_id = ObjectId(clique_id)
    except TypeError:
        print 'error input type in get_similar_cliques_on_nodes'
        sys.exit(1)

    n_nodes = len(clique_nodes)
    combination_of_nodes = list(itertools.combinations(clique_nodes, n_nodes - k))

    query = {"_id": {"$ne": clique_id}, "count": {"$gt": n_nodes - k}, "$or": []}
    for comb in combination_of_nodes:
        query["$or"].append({"nodes": {"$all": list(comb)}})
    #print(query)
    start = time.time()

    cliques_cursor = db[collection].find(query)

    end = time.time()
    print('for one query', end - start)

    # cliques = [c for c in cliques_cursor]
    return cliques_cursor


def get_cliques(ids):
    try:
        oids = map(ObjectId ,ids)
    except InvalidId:
        # print 'not valid ids passed to get_cliques in clique_dao'
        # oids = ids
        return [] #for compatibilty with community return []
    except Exception:
        print 'generic error difficult to explain'
        oids = ids
    #db = DbInstance(27017, 'twitter').getDbInstance()
    db = DbInstance().getDbInstance()
    cliques = db[collection].find({'_id': {'$in': oids}})
    return cliques


def create_dataset(dimension= 1000000):
    # type: (int)->Cursor
    drop()
    clqs = get_maximal_cliques_on_valid_users().sort([('count', DESCENDING)]).limit(dimension)
    #db = DbInstance(27017, 'twitter').getDbInstance()
    db = DbInstance().getDbInstance()
    cliques = list(clqs)
    clqs.close()
    db['comms'].insert_many(cliques)
    import pymongo
    db['comms'].create_index([('nodes', pymongo.ASCENDING)])
    print 'dataset created'


def count_destribuition_per_num_of_nodes_in_cliques():
    #db = DbInstance(27017, 'twitter').getDbInstance()
    db = DbInstance().getDbInstance()
    pipeline = [ {"$group": {'_id': '$count', 'count': {'$sum':1}  }  }  ]
    destribuition = db[collection].aggregate(pipeline)
    return destribuition

def clique_destribuition_per_user():
    #db = DbInstance(27017, 'twitter').getDbInstance()
    db = DbInstance().getDbInstance()
    pipeline = [
        {'$unwind':'$nodes'},
        {'$group':{'_id':'$nodes', 'count':{'$sum': 1 } } },
        {'$sort':{'count': -1}}
    ]
    return db[collection].aggregate(pipeline)

#useful? ??
def ordered_data_set_aggregaion(dim):
    # type:(int)->Cursor
    #db = DbInstance(27017, 'twitter').getDbInstance()
    db = DbInstance().getDbInstance()
    pipeline = [{"$sort":{"count":-1}},{"$limit":dim}]
    data_set = db[collection].aggregate(pipeline)
    return data_set

def mean_of_clq_per_user():
    destrib = clique_destribuition_per_user()
    values_clq_per_user = [doc['count'] for doc in destrib]
    import numpy as np
    mean_destrib = np.array(values_clq_per_user).mean()
    print mean_destrib




#print mean_of_clq_per_user()

#print len(list(clique_destribuition_per_user() ) )
#print list(clique_destribuition_per_user())

# print get_maximal_cliques_on_valid_users().count()
# clq = {
#     "_id": ObjectId("5946a56ef810d56922435acd"),
#     "nodes": [
#         "1960436850",
#         "1716578040",
#         "196758914",
#         "24871896",
#         "63525184",
#         "294200866",
#         "162779090",
#         "298537212",
#         "568248162"
#     ],
#     "count": 9,
#     "pt": "final"
# }


#create_dataset(100)
# c =get_limit_maximal_cliques(10)
# print list(c)
# c.close()