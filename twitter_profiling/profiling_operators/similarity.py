#from twitter_graph.graph import UndirectedGraph
import networkx as nx
from networkx.algorithms import approximation
import matplotlib.pyplot as plt
from twitter_graph import graph

def jaccard(set_a, set_b):
    set_a = set(set_a)
    set_b = set(set_b)

    return float(len(set_a.intersection(set_b) ) ) / float(len(set_a.union(set_b) ) )



def isomorphism_measure(G, H): #high result means low similarity
    #type:(nx.Graph, nx.Graph)->int
    G_clone = G.copy()
    H_clone = H.copy()

    D = __simmetric_difference(G_clone, H_clone)
    if D.nodes > 80 and D.edges > 50: vertex_cover = approximation.min_weighted_vertex_cover(D)
    else: vertex_cover = graph.minimum_vertex_cover(D)

    print 'vertex cover :',vertex_cover
    #vertex cover is equal to the min nuber of nodes to delete to obtain 2 isomorphic graphs
    g_nodes = [n for n in G]
    h_nodes = [n for n in H]
    print 'edges',G.edges
    print 'edges',H.edges
    common_nodes = set(g_nodes).intersection(h_nodes)
    #how map vertex_cover to a similarity value or dist value
    #similarity = len(common_nodes)/float(len(vertex_cover) )

    return len(vertex_cover) #similarity


def __simmetric_difference(G, H):
    # type: (nx.Graph, nx.Graph) -> nx.Graph
    g_nodes = [n for n in G]
    print g_nodes
    h_nodes = [n for n in H]
    print h_nodes
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
    D = None
    #nx.draw(symm_diff_graph, with_labels=True)
    #plt.show()
    return symm_diff_graph