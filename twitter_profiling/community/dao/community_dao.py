from twitter_mongodb.twitterdb_instance import DbInstance
from twitter_clique.clique_dao import get_cliques
from bson.objectid import ObjectId
from pymongo.cursor import Cursor
import itertools, time, sys

collection = 'comms'
db_name = 'twitter'
port = 27017

def get_similar_community_on_nodes(_id, nodes, k=1):  # check if id needs casting to ObjIdMongo
    #type: (str, list[str], int)->Cursor
    db = DbInstance(port, db_name).getDbInstance()
    # verifing input
    try:
        nodes = list(nodes)
        _id = ObjectId(_id)
    except TypeError:
        print 'error input type in get_similar_cliques_on_nodes'
        sys.exit(1)

    n_nodes = len(nodes)
    combination_of_nodes = list(itertools.combinations(nodes, n_nodes - k))

    query = {"_id": {"$ne": _id}, "count": {"$gt": n_nodes - k}, "$or": []}
    for comb in combination_of_nodes:
        query["$or"].append({"nodes": {"$all": list(comb)}})
    #print(query)
    start = time.time()

    cliques_cursor = db[collection].find(query)

    end = time.time()
    print('for one query', end - start)

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
        print 'deleting cliques'
    except:
        query = {"com_id": {"$in": ids}}
        print ' deleting communities'

    print 'deleted', db[collection].delete_many(query).deleted_count
    print 'request to delete', len(ids)
    print '**********************************************************'
    file = open('deleted_ids.txt', 'a')

    clqs = list(get_cliques(ids) )
    print 'cliques retrieved with the id just deleted =', len(clqs)
    if len(clqs) > 0:
        print 'pdpdpd mongo shit'
    for id in ids :
        file.write(str(id)+',')
    file.close

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
    doc['users'] = list(doc['users'])
    doc['cliques'] = list(doc['cliques'])
    doc['count'] = len(doc['users'])
    del doc['weighted_profile']
    # del doc['interests_data']
    del doc['profile']
    return doc