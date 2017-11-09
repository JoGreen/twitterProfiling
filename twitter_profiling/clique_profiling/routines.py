from twitter_profiling.clique_profiling.clique import Clique
from twitter_profiling.community.community import Community
from twitter_profiling.clique_profiling.clique_graph import neighbour_graph
import  operator, sys, networkx as nx, numpy as np

# import findspark
# findspark.init()
# os.environ["PYSPARK_SUBMIT_ARGS"] = (
#     "--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 pyspark-shell")


# try:
#     from graphframes import graphframe as GF
# except ImportError as ex:
#     print "Can not import Spark Modules", ex
#     sys.exit(1)

def find_attractor(G, pagerank_with_node_weight= True):#node with max gravity
    # type:(nx.DiGraph, bool)->(str, float)
    # strategies: -> pagerank with weigthed graph or mean of weightded in link
    mapping_gravity= {}

    if pagerank_with_node_weight is False:
        mapping_gravity = nx.pagerank_numpy(G)
        hubs, authorities = nx.hits_numpy(G)
        hubs = sorted(hubs.items(), key=operator.itemgetter(1), reverse=True)
        authorities = sorted(authorities.items(), key=operator.itemgetter(1), reverse=True)
        print hubs
        print
        print authorities

    else:
        attraction_forces = {}
        for dst in G.nodes:
            neighbors = G.predecessors(dst)
            orbital_gravities = []
            for src in neighbors:
                orbital_gravities.append(G.get_edge_data(src, dst)['weight'])
            if len(orbital_gravities) is 0 : orbital_gravities.append(0.)
            mean = np.mean(orbital_gravities)
            attraction_forces[dst]= mean
        mapping_gravity = nx.pagerank_numpy(G, personalization= attraction_forces)

    sorted_gravities = sorted(mapping_gravity.items(), key=operator.itemgetter(1), reverse= True)
    return sorted_gravities


def expand(clq):
    # type:(Clique)->list[Community]
    pass
    G = neighbour_graph(clq)
    com_map = {}
    attractor1 = find_attractor(G, pagerank_with_node_weight= False)
    attractor2 = find_attractor(G)
    print attractor1
    print attractor2

    #if nx.number_connected_components() is G.number_of_nodes():
     #   return com_map.values()




