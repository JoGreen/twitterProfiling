from clique import Clique
import networkx as nx
import matplotlib.pyplot as plt

def neighbour_graph(clq):
    # type:(Clique)->nx.DiGraph
    neighb = clq.get_neighbours(k=1, cohesion_type='g') # each elem has enough cohesion
    edges = [(clq.get_id(), n.get_id()) for n in neighb]
    G = nx.DiGraph()
    G.add_edges_from(edges)
    nx.draw(G, with_labels=True)
    plt.show()
    return G


# valauta se la differenza tra graph cohesion e vector cohesion elimina cmq le due clique meno coese