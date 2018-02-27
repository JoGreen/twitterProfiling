from persistance_mongo_layer.twitterdb_instance import DbInstance
from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo.cursor import Cursor
import itertools, time, sys, subprocess
from persistance_mongo_layer import pamir_connection_data as db_connection_data

collection = 'comms'
db_name = db_connection_data.db_name
#port  = db_connection_data.port
threeshold_for_huge_community = 120

authentication_string = ' --username ' + db_connection_data.user + ' --password ' + db_connection_data.pwd + ' '

def get_similar_community_on_nodes(_id, nodes, k=1):  # check if id needs casting to ObjIdMongo
    #type: (str, list[str], int)->Cursor
    #db = DbInstance(port, db_name).getDbInstance()
    db = DbInstance().getDbInstance()
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


    if (n_nodes - k) < 1:
        n_nodes = k + 1
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
    #db = DbInstance(port, db_name).getDbInstance()
    db = DbInstance().getDbInstance()
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


def insert(comms):
    # type:(list[Community])->None
    if comms == None or len(comms) == 0:
        return None # it happens when a clique/communitiy has no neighbours

    db = DbInstance().getDbInstance()
    #db = DbInstance(port, db_name).getDbInstance()
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
    db = DbInstance().getDbInstance()
    comms = db[collection].find({'cliques': {'$in': cliques_ids}})
    return comms

def get_communities(comm_ids):
    try:
        comm_ids = list(comm_ids)
    except TypeError:
        print 'need an iterable to get communities with specific cliques from db '
        sys.exit(1)
    db = DbInstance().getDbInstance()
    comms = db[collection].find({'com_id': {'$in': comm_ids}})
    return comms

def get_count():
    db = DbInstance().getDbInstance()
    return db[collection].count()

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
    del doc['clique']
    del doc ['knowledge_graph']
    return doc

def drop():
    #db = DbInstance(port, db_name).getDbInstance()
    db = DbInstance().getDbInstance()
    db[collection].drop()
    print 'collection '+collection+' dropped'


def get_all():
    #db = DbInstance(port, db_name).getDbInstance()
    db = DbInstance().getDbInstance()
    return db[collection].find({}, no_cursor_timeout=True)

def do_dump(limit_dataset, iter):
    # type:(int, int)->None
    cmd = 'mongodump'+authentication_string+ '--db '+db_name+ ' --collection comms -o dumps/dump_'+str(limit_dataset)+'_iter'+str(iter)
    print subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell= True)

def do_restore(limit_dataset, iter):
    # type:(int, int)->None
    drop()
    cmd = 'mongorestore'+authentication_string+ '--db '+db_name+ ' --collection comms dumps/dump_' + str(limit_dataset) + '_iter' + str(iter)+'/twitter/comms.bson'
    print subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)

