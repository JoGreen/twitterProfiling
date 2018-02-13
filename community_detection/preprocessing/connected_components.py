import igraph
import sys, itertools
from persistance_mongo_layer.twitterdb_instance import DbInstance
from community_detection.clique_profiling.clique import Clique
from bson.objectid import ObjectId
from tqdm import tqdm


collection = "cliques2analyze"
id_map = {'gnegne': -1}
reverse_map = {}

def node_index(clq_id):
    if clq_id in id_map.keys():
        node = id_map[clq_id]
    else:
        node = max(id_map.values()) + 1
        id_map[clq_id] = node
        reverse_map[node] = clq_id

    return node

def get_all_connected_components():
    db = DbInstance().getDbInstance()

    cliques = db['cliques2analyze'].find({}, no_cursor_timeout=True)#.limit(200)
    #pool = Pool(cpu_count() - 1)
    #graphs = map(create_cliques_graph_of, cliques)
    graphs = []
    g = igraph.Graph()
    for c in tqdm(cliques):
        create_cliques_graph_of(c, g)
        #graphs.append(create_cliques_graph_of(c, g) )
    #G = igraph.Graph().union(graphs)
    cliques.close()
    return g.clusters()

def create_cliques_graph_of(clq, g):
    # type:(dict)->igraph.Graph
    #clique = constructor(clq)
    clq_id = str(clq['_id'])

    if not clq_id in id_map.keys():
        node = node_index(clq_id)
        g.add_vertex(node)
    else:
        node = node_index(clq_id)

    connected_cliques_cur = get_similar_community_on_nodes(clq_id, clq['nodes'], k= Clique.k)
    connected_cliques = list(connected_cliques_cur)
    connected_cliques_cur.close()
    for c in connected_cliques:
        c2_id = str(c['_id'])
        if not c2_id in id_map.keys():
            node2 = node_index(c2_id)
            g.add_vertex(c2_id)
        else:
            node2 = node_index(c2_id)

        g.add_edge(node, node2 )
    #print 'done'

    return g

def get_similar_community_on_nodes(_id, nodes, k=1):  # check if id needs casting to ObjIdMongo
    #type: (str, list[str], int)->Cursor
    db = DbInstance().getDbInstance()
    # verifing input
    try:
        nodes = list(nodes)
        _id = ObjectId(_id)
        n_nodes = len(nodes)
        query = {
            "_id": {"$ne": _id},
            #"count": {"$lt": threeshold_for_huge_community},# "count": {"$gt": n_nodes - k},
            "$and":[
                {"$or": [{"accept_in_links": True}, {"accept_in_links":{"$exists": False} } ] },
                {"$or":[]}
            ]
        }
    except TypeError:
        print 'nodes has to be an iterable---wrong type parameter in get_similar_community_on_nodes function'
        sys.exit(1)

    if(n_nodes - k) < 1:
        n_nodes = k + 1
    combination_of_nodes = itertools.combinations(nodes, n_nodes - k)

    for comb in combination_of_nodes:
        query["$and"][1]["$or"].append({"nodes": {"$all": list(comb)}})

    cliques_cursor = db[collection].find(query , no_cursor_timeout=True)
    return cliques_cursor

#number of connected components 2838 with k=1
#number of connected components 1833 with k=2 => just 468 with more than one node

def translate_and_persist(connected_components):
    db = DbInstance().getDbInstance()
    collection = db['acm_connected_components']

    mapped_components = []
    for component in connected_components:
        mapped_component = {'nodes':[]}
        for index, vertex in enumerate(component):
            mapped_component['nodes'].append(reverse_map[vertex])
        if len(mapped_component['nodes']) > 1:
            mapped_components.append(mapped_component)

    collection.insert_many(mapped_components)
    return mapped_components


connected_components = get_all_connected_components()
print 'number of connected components', len(connected_components)
for comp in connected_components:
    print len(comp)
    print comp
translated_components = translate_and_persist(connected_components)
print 'done'


