#from twitter_graph.graph import UndirectedGraph
import itertools
import numpy as np
import sys

import networkx as nx
from networkx.algorithms import approximation
from scipy.spatial.distance import cosine
from sklearn.metrics.pairwise import cosine_distances
from sklearn.metrics.pairwise import cosine_similarity


def jaccard(set_a, set_b):
    set_a = set(set_a)
    set_b = set(set_b)

    return float(len(set_a.intersection(set_b) ) ) / float(len(set_a.union(set_b) ) )

def vector_distance(vectors):
    #type:(list)->float
    try:
        a = list(vectors[0])
        b= list(vectors[1])
    except TypeError:
        print 'vector distance needs array like inputs'
        print vectors
        sys.exit(1)
    return cosine(a,b)

def vector_distance_between_all(vectors):
    #type:(list)->float
    try:
        vectors = np.array(vectors)
    except TypeError:
        print 'vector distance needs array like inputs'
        print vectors
        sys.exit(1)
    if len(vectors) == 1:
        return [0.]
    result_matrix = cosine_distances(vectors)
    values = itertools.chain(*(result_matrix.diagonal(l).tolist()  for l in range(1,len(vectors)) ) )
    return values

def vector_similarity(a,b):
    # type:(list, list)->float
    try:
        a = list(a)
        b= list(b)
    except TypeError:
        print 'wrong type input vector similarity function'
        sys.exit(1)
    matrix = [a,b]
    #matrix = np.array(matrix)
    result = cosine_similarity(matrix)
    return result[0][1] # array([[ 1.        ,  0.38709324],
                                #[ 0.38709324,  1.        ]]) that s an example of result


def vector_similarity_between_all(vectors):
    # type:(list)->float
    try:
        vectors = np.array(vectors)
    except TypeError:
        print 'wrong type input vector similarity function'
        sys.exit(1)
    #matrix = [a,b]
    #matrix = np.array(matrix)
    result = cosine_similarity(vectors)
    return result

def isomorphism_measure(G, H): #high result means low similarity
    #type:(nx.Graph, nx.Graph)->int
    G_clone = G.copy()
    H_clone = H.copy()

    D = __simmetric_difference(G_clone, H_clone)
    if D.number_of_nodes() > 80 or D.number_of_edges() > 80:
        vertex_cover = len(approximation.min_weighted_vertex_cover(D) )
    else:
        #if D.number_of_nodes() > 80 or D.number_of_edges() > 50:
        vertex_cover = len(approximation.min_weighted_vertex_cover(D) )
        #vertex_cover = graph.dense_connected_components_minimum_vertex_cover(D) # not work !!!!!!!!!!!!!!!!
        #else: vertex_cover = graph.minimum_vertex_cover(D)

    print 'vertex cover cardinality', vertex_cover
    #vertex cover is equal to the min nuber of nodes to delete to obtain 2 isomorphic graphs
    g_nodes = [n for n in G]
    h_nodes = [n for n in H]
    print 'edges',G.edges
    print 'edges',H.edges
    #common_nodes = set(g_nodes).intersection(h_nodes)
    #how map vertex_cover to a similarity value or dist value
    #similarity = len(common_nodes)/float(len(vertex_cover) )
    return vertex_cover #similarity


def __simmetric_difference(G, H):
    # type: (nx.Graph, nx.Graph) -> nx.Graph
    g_nodes = [n for n in G]
    #print g_nodes
    h_nodes = [n for n in H]
    #print h_nodes
    nodes2add2g = set(h_nodes).difference(set(g_nodes))
    nodes2add2h = set(g_nodes).difference(set(h_nodes))

    #add nodes beacuase this op needs the same vertices set
    G.add_nodes_from(nodes2add2g)
    H.add_nodes_from(nodes2add2h)

    D = nx.symmetric_difference(G,H)

    print 'arcs in symmetric difference',len(D.edges)
    symm_diff_graph = nx.Graph()
    symm_diff_graph.add_edges_from(D.edges)
    print 'nodes in symmetric difference', len(symm_diff_graph.nodes)
    print 'edges in symmetric difference', len(symm_diff_graph.edges)
    #nx.draw(symm_diff_graph)
    #plt.show()
    D = None
    #nx.draw(symm_diff_graph, with_labels=True)
    #plt.show()
    return symm_diff_graph


#print list(vector_distance_between_all([[1,2,3],[2,3,4],[1,3,4]]) )