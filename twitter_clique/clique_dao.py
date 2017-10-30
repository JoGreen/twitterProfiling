from twitter_mongodb.twitterdb_instance import DbInstance
import itertools
from bson.objectid import ObjectId
from pymongo.cursor import Cursor
from twitter_profiling.user_profiling.user_dao import UserDao
from twitter_profiling.user_profiling.profile_dao import ProfileDao
import time, sys

def get_maximal_cliques(grower_than = 6):
    # type: (int) -> Cursor
    db = DbInstance(27017, 'twitter').getDbInstance()
    cliques = db['cliques2analyze'].find({"count": {"$gt": grower_than}})
    # print(type(cliques) )
    return cliques


def get_limit_maximal_cliques(limit, grower_than = 6):
    # type: (int, int) -> Cursor
    clq = get_maximal_cliques(grower_than)
    clq.limit(limit)
    print(clq.count(), 'total cliques')
    return clq

def get_limit_maximal_cliques_on_valid_users(limit, gt= 7):
    clqs = get_maximal_cliques_on_valid_users()
    clqs.limit(limit)
    return clqs

def get_maximal_cliques_on_valid_users(gt= 7): # doesn t work
    us2avoid = ProfileDao().get_users_without_interests()
    print us2avoid.count()
    us = [u for u in us2avoid]
    #for i in range(0, 1929): us.pop()
    print len(us)
    #us=['10','19']
    db = DbInstance(27017, 'twitter').getDbInstance()

    cliques = db['cliques2analyze'].find({'nodes':{'$nin': us} ,
                                          'count':{'$gt':gt} }, no_cursor_timeout= True)
    return cliques

def get_maximal_clique_on_specific_users_friendship_count(friendship_count, gt = 7):
    us = UserDao().get_users_with_lt_friends(friendship_count)
    users = [u['id'] for u in us]

    db = DbInstance(27017, 'twitter').getDbInstance()

    cliques = db['cliques2analyze'].find({'nodes': {'$in': users}, 'count':{'$gt':gt}}, no_cursor_timeout=True)

    return cliques

def get_limit_maximal_clique_on_specific_users_friendship_count(friendship_count, limit):
    # type:(int, int)->Cursor
    cliques = get_maximal_clique_on_specific_users_friendship_count(friendship_count)
    cliques.limit(limit)
    print(cliques.count())
    return cliques


def get_similar_cliques_on_nodes(clique_id, clique_nodes, k=1):  # check if id needs casting to ObjIdMongo
    #type: (str, list[str], int)->Cursor
    db = DbInstance(27017, 'twitter').getDbInstance()
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
    print(query)
    start = time.time()

    cliques_cursor = db['cliques2analyze'].find(query)

    end = time.time()
    print('for one query', end - start)

    # cliques = [c for c in cliques_cursor]
    return cliques_cursor


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
#
# cliques = get_similar_cliques_on_nodes(clq, 2)
# # for cliques in cliques_cursors:
# for c in cliques:
#     print('clique:', c['_id'])
# print('end')
# # c = getLimitMaximalCliques(100)
