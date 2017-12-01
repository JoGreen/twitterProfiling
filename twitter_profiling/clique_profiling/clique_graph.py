from clique import Clique
import networkx as nx, operator
# import matplotlib.pyplot as plt
from multiprocessing import Pool, freeze_support
#import itertools
#from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, FloatType, StringType, StructField
#from graphframes import graphframe as gf

max_depth = 2 #increase this value need to re-implement how pass the expanded_cliques variable

# two times the same function one create graph where nodes are id, the other one use clique or community objects as vertices
def neighbour_graph_with_id(clq, visited, lev=1):
    # type:(Clique, set([str]), int)->(nx.DiGraph, set([str]) )
    # print 'level',lev,': building_graph ...'
    neighb2_and_cohesion = clq.get_neighbours(k=1) # return a list of dict with clique and cohesion fields
    if neighb2_and_cohesion is None:
        neighb2_and_cohesion= []

    map_num_nodes = {clq.get_id(): len(clq.users) }
    for doc in neighb2_and_cohesion:
        map_num_nodes.update(doc["user_count"])
    # n2 = sorted(neighb2_and_cohesion, key=operator.itemgetter('cohesion'))
    # print [(e['cohesion'], e['clique'].get_id() ) for e in n2]

    H = nx.DiGraph()


    if lev < max_depth :
        neighbours = [(n['clique'], visited, lev + 1) for n in neighb2_and_cohesion
                      if not n['clique'].get_id() in visited] # no nodes already expanded

        if len(neighb2_and_cohesion) > 0:
            # pool= Pool(8)
            # graphs = pool.map(launcher, neighbours)
            # pool.close()
            # H = nx.compose_all(graphs)

            # for n in neighbours:
            graphs_and_nodes_count = map(launcher, neighbours) # nx.compose(H, neighbour_graph(*n) )
            graphs = []
            for t in graphs_and_nodes_count:
                graphs.append(t[0])
                map_num_nodes.update(t[1])

            if len(graphs) > 0:
                H = nx.compose_all(graphs)


    #should consider to check for communities presence with too low cohesion to avoid to have entering edges.
    # at the moment they could have in edges also if check_acceptance return False, but if its cohesion don t gr
    edges = [(clq.get_id(), n['clique'].get_id(), clq.get_profile_vector_similarity_with(n['clique']) )
             for n in neighb2_and_cohesion] #used because it presents also nodes already expanded


    G = nx.DiGraph()
    G.add_node(clq.get_id() ) # if edges is empty prevent to have a 0 nodes graph
    G.add_weighted_edges_from(edges)
    G = nx.compose(G, H)
    if lev is 1:
        expanded_cliques = [n[0].get_id() for n in  neighbours]
        expanded_cliques.append(clq.get_id() )
        expanded_cliques = set(expanded_cliques)
        # delete_external_nodes(G)
        print
        print 'graph built.'
        # return a  graph and a list of clique ids and a map with number of users for community
        return G, expanded_cliques, map_num_nodes

    return G, map_num_nodes

def neighbour_graph(clq, visited, lev=1):
    # type:(Clique, set([str]), int )->(nx.DiGraph, set([str]))
    neighb2_and_cohesion = clq.get_neighbours(k=1)
    if neighb2_and_cohesion is None:
        neighb2_and_cohesion = []

    H = nx.DiGraph()
    print 'level', lev

    if lev < max_depth:
        neighbours = [(n['clique'], visited, lev + 1) for n in neighb2_and_cohesion
                      if not n['clique'].get_id() in visited]
        pool= Pool(8)
        graphs = pool.map(launcher, neighbours)
        pool.close()
        H = nx.compose_all(graphs)

        # for n in neighbours:
        #     H = nx.compose(H,neighbour_graph(*n) )

    edges = [(clq, n['clique'], clq.get_profile_vector_similarity_with(n['clique']) )
             for n in neighb2_and_cohesion]
    G = nx.DiGraph()
    G.add_weighted_edges_from(edges)
    G = nx.compose(G, H)
    if lev is 1:
        expanded_cliques = [n['clique'].get_id() for n in neighb2_and_cohesion]
        expanded_cliques.append(clq.get_id())
        expanded_cliques = set(expanded_cliques)
        # delete_external_nodes(G)
        return G,  expanded_cliques # returna  graph and a list of clique ids

    return G

def launcher(t):
    #type: (tuple)->nx.DiGraph
    return neighbour_graph_with_id(*t)

def delete_external_nodes(G):
    # type:(nx.DiGraph)->None
    for n in G.copy().nodes:
        if G.out_degree(n) is 0 and G.in_degree(n) is 1:
            G.remove_node(n)


# def spark_pagerank(G):
#     # type:(nx.DiGraph)-> None
#     spark = SparkSession.builder.master("local[4]") \
#         .appName("graph_clique") \
#         .getOrCreate()
#     edges = [(e[0], e[1])for e in G.edges]
#     vertices = [(v,'node')for v in G.nodes]
#     #print vertices
#     #print edges
#     schema_v = StructType([StructField('id', StringType(),StructField('type', StringType() ))])
#     schema_e = StructType([
#             StructField('src', StringType() ),
#             StructField('dst', StringType() )
#         ])
#     e = spark.createDataFrame([ee for ee in edges], schema_e)
#     v.show()
#     print '-------'
#     # v2 = spark.createDataFrame([('a', Clique(['233'], 100))],['id', ])
#     v = spark.createDataFrame([vv for vv in vertices], schema_v)
#     GF = gf.GraphFrame(v, e)
#     GF.vertices.show()
#     print 'compute page rank'
#     results = GF.pageRank(maxIter=1)
#     print 'computed pagerank'
#     results.vertices.show()
#     spark.stop()
#     return results
# valauta se la differenza tra graph cohesion e vector cohesion elimina cmq le due clique meno coese


def get_edge_star(l):
    return get_edge(*l)

def get_edge(com1, com2):
    return (com1.get_id(), com2['clique'].get_id(), com1.get_profile_vector_similarity_with(com2['clique']) )