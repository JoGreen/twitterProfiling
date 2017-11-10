from clique import Clique
import networkx as nx, operator
import matplotlib.pyplot as plt
from multiprocessing import Pool
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, FloatType, StringType, StructField
from graphframes import graphframe as gf

max_depth = 2

def neighbour_graph(clq, lev=1):
    # type:(Clique)->(nx.DiGraph, list[str])
    neighb2_and_cohesion = clq.get_neighbours(k=1)
    if neighb2_and_cohesion is None:
        neighb2_and_cohesion= []

    # n2 = sorted(neighb2_and_cohesion, key=operator.itemgetter('cohesion'))
    # print [(e['cohesion'], e['clique'].get_id() ) for e in n2]

    H = nx.DiGraph()
    print 'level', lev

    if lev < max_depth:
        neighbours = [(n['clique'], lev + 1) for n in neighb2_and_cohesion]
        pool= Pool(8)
        graphs = pool.map(launcher, neighbours)
        pool.close()
        H = nx.compose_all(graphs)

        # for n in neighbours:
        #     H = nx.compose(H,neighbour_graph(*n) )

    edges = [(clq.get_id(), n['clique'].get_id(), clq.get_profile_vector_similarity_with(n['clique']) )
             for n in neighb2_and_cohesion]
    G = nx.DiGraph()
    G.add_weighted_edges_from(edges)
    G = nx.compose(G, H)
    if lev is 1:
        delete_external_nodes(G)
        expanded_cliques = [n['clique'].get_id() for n in neighb2_and_cohesion]
        expanded_cliques.append(clq.get_id())
        return G,  expanded_cliques # returna  graph and a list of clique ids
        # nx.draw(G, with_labels=True )
        # plt.show()
    ###########################################################################

        # pgr_values = nx.pagerank(G, weight='weight', tol=1e-09, max_iter=500)
        # pgr_values_numpy = nx.pagerank_numpy(G)
        # # pgr_values.sort(reverse=True)
        # # pgr_values_numpy.sort(reverse=True)
        # sorted_pgr_numpy = sorted(pgr_values_numpy.items(), key=operator.itemgetter(1), reverse=True)
        # sorted_pgr = sorted(pgr_values.items(), key=operator.itemgetter(1))
        # print sorted_pgr
        # print
        # print sorted_pgr_numpy
        # degree_ratio = {}
        # for n in G.nodes:
        #     out_degree = G.out_degree(n)
        #     if out_degree is 0 :
        #         out_degree = 0.9
        #     degree_ratio[n] = G.in_degree(n)/float(out_degree )
        # sorted_degree_ratio = sorted(degree_ratio.items(), key=operator.itemgetter(1))
        # print sorted_degree_ratio
    ####################################################################
    return G

def launcher(t):
    #type: (tuple)->nx.DiGraph
    return neighbour_graph(*t)

def delete_external_nodes(G):
    # type:(nx.DiGraph)->None
    for n in G.copy().nodes:
        if G.out_degree(n) is 0 and G.in_degree(n) is 1:
            G.remove_node(n)


def spark_pagerank(G):
    # type:(nx.DiGraph)-> None
    spark = SparkSession.builder.master("local[4]") \
        .appName("graph_clique") \
        .getOrCreate()
    edges = [(e[0], e[1])for e in G.edges]
    vertices = [(v,'node')for v in G.nodes]
    #print vertices
    #print edges
    schema_v = StructType([StructField('id', StringType(),StructField('type', StringType() ))])
    schema_e = StructType([
            StructField('src', StringType() ),
            StructField('dst', StringType() )
        ])
    e = spark.createDataFrame([ee for ee in edges], schema_e)
    v.show()
    print '-------'
    # v2 = spark.createDataFrame([('a', Clique(['233'], 100))],['id', ])
    v = spark.createDataFrame([vv for vv in vertices], schema_v)
    GF = gf.GraphFrame(v, e)
    GF.vertices.show()
    print 'compute page rank'
    results = GF.pageRank(maxIter=1)
    print 'computed pagerank'
    results.vertices.show()
    spark.stop()
    return results
# valauta se la differenza tra graph cohesion e vector cohesion elimina cmq le due clique meno coese