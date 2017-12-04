from twitter_mongodb.twitterdb_instance import DbInstance
from twitter_clique.clique_dao import get_cliques
from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo.cursor import Cursor
import itertools, time, sys

collection = 'comms'
db_name = 'twitter'
port = 27017

threeshold_for_huge_community = 120

def get_similar_community_on_nodes(_id, nodes, k=1):  # check if id needs casting to ObjIdMongo
    #type: (str, list[str], int)->Cursor
    db = DbInstance(port, db_name).getDbInstance()
    # verifing input
    try:
        nodes = list(nodes)
        _id = ObjectId(_id)
        n_nodes = len(nodes)
        query = {
            "_id": {"$ne": _id},
            "count": {"$lt": threeshold_for_huge_community},# "count": {"$gt": n_nodes - k},
            "$and":[
                {"$or": [{"accept_in_links": True}, {"accept_in_links":{"$exists": False} } ] },
                {"$or":[]}
            ]
        }
    except TypeError:
        print 'nodes has to be an iterable---wrong type parameter in get_similar_community_on_nodes function'
        sys.exit(1)
    except InvalidId:
        n_nodes = len(nodes)
        query = {
            "com_id": {"$ne": _id},
            "count": {"$lt": threeshold_for_huge_community},#{"$gt": n_nodes - k},# "count": {"$lt": threeshold_for_huge_community - k}, #"count": {"$gt": n_nodes - k},
            "$and":[
                {"$or": [{"accept_in_links": True}, {"accept_in_links":{"$exists": False} } ] },
                {"$or":[]}
            ]
        }

   # n_nodes = len(nodes)
    combination_of_nodes = itertools.combinations(nodes, n_nodes - k)


    for comb in combination_of_nodes:
        query["$and"][1]["$or"].append({"nodes": {"$all": list(comb)}})

    # start = time.time()

    cliques_cursor = db[collection].find(query)

    # end = time.time()
    # print('for one query', end - start)

    # cliques = [c for c in cliques_cursor]
    return cliques_cursor


def delete(ids):
    # type:(list[str])->None
# accept ids of communities or ids of cliques, never at the same time
    db = DbInstance(port, db_name).getDbInstance()
    try:
        ids = list(ids)

    except TypeError:
        print 'error input type in delete communities'
        sys.exit(1)
    try:
        ids = map(ObjectId, ids)
        query = {"_id": {"$in": ids}}
        #print 'deleting cliques'
    except InvalidId:
        query = {"com_id": {"$in": ids}}
        #print ' deleting communities'


    db[collection].delete_many(query)
    # print 'request to delete', len(ids)
    #print '**********************************************************'
    ###file = open('deleted_ids.txt', 'a')
    ###ids_to_write = map(str, ids)
    ###string_to_write = ",".join(ids_to_write)
    ###file.write(string_to_write)
    ###file.close

def insert(comms):
    # type:(list[Community])->None
    if comms == None or len(comms) == 0:
        return None # it happens when a clique/communitiy has no neighbours

    db = DbInstance(port, db_name).getDbInstance()
    try:
        ids = list(comms)
    except TypeError:
        print 'error input type in get_similar_cliques_on_nodes'
        sys.exit(1)
    docs = map(__converter__, comms)
    # print docs
    inserted = db[collection].insert_many(docs)
    print 'inserted new communities ids', len(inserted.inserted_ids)

def get_communities_with_specific_cliques(cliques_ids):
    try:
        cliques_ids = list(cliques_ids)
    except TypeError:
        print 'need an iterable to get communities with specific cliques from db '
        sys.exit(1)
    db = DbInstance(port, db_name).getDbInstance()
    comms = db[collection].find({'cliques': {'$in': cliques_ids}})
    return comms

def get_communities(comm_ids):
    try:
        comm_ids = list(comm_ids)
    except TypeError:
        print 'need an iterable to get communities with specific cliques from db '
        sys.exit(1)
    db = DbInstance(port, db_name).getDbInstance()
    comms = db[collection].find({'com_id': {'$in': comm_ids}})
    return comms

def __converter__(com):
    # type:(Community)-> dict
    doc = com.__dict__
    try:
        doc['nodes'] = list(doc['users'])
    except KeyError:
        print doc
    doc['cliques'] = list(doc['cliques'])
    doc['count'] = len(doc['users'])
    del doc['weighted_profile']
    # del doc['interests_data']
    del doc['profile']
    del doc['users']
    return doc