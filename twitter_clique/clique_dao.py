from twitter_mongodb.twitterdb_instance import DbInstance
import itertools
from bson.objectid import ObjectId
from pymongo.cursor import Cursor
from twitter_profiling.user_profiling.user_dao import UserDao
import time

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



def get_maximal_clique_on_specific_users_friendship_count(friendship_count, gt = 7):
    us = UserDao().get_users_with_lt_friends(friendship_count)
    users = [u['id'] for u in us]

    db = DbInstance(27017, 'twitter').getDbInstance()

    cliques = db['cliques2analyze'].find({'nodes': {'$in': users}, 'count':{'$gt':gt}}, no_cursor_timeout=True)

    return cliques

def get_limit_maximal_clique_on_specific_users_friendship_count(friendship_count, limit):
    cliques = get_maximal_clique_on_specific_users_friendship_count(friendship_count)
    cliques.limit(limit)
    print(cliques.count())
    return cliques


def get_similar_cliques_on_nodes(clique, k=1):  # check if id needs casting to ObjIdMongo
    db = DbInstance(27017, 'twitter').getDbInstance()
    # verifing input
    clique['nodes'] = list(clique['nodes'])
    clique['_id'] = ObjectId(clique['_id'])

    n_nodes = len(clique['nodes'])
    combination_of_nodes = list(itertools.combinations(clique['nodes'], n_nodes - k))

    query = {"_id": {"$ne": clique["_id"]}, "count": {"$gt": n_nodes - k}, "$or": []}
    for comb in combination_of_nodes:
        query["$or"].append({"nodes": {"$all": list(comb)}})
    print(query)
    start = time.time()

    cliques_cursor = db['cliques2analyze'].find(query)

    end = time.time()
    print('for one query', end - start)

    cliques = [c for c in cliques_cursor]
    return cliques




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
