from twitter_profiling.clique_profiling.clique import Clique
from twitter_profiling.community.community import Community
from twitter_profiling.clique_profiling.clique_graph import neighbour_graph
from twitter_clique import clique_dao
import  operator, sys, networkx as nx, numpy as np, matplotlib.pyplot as plt

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
    # strategies: -> pagerank -> undestand better weights on edges and on nodes what do they mean in term of result
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
    comms = {}
    G, expanded = neighbour_graph(clq)
    print 'expanded cliques', len(expanded)
    while len(list(nx.weakly_connected_components(G) ) ) < len(G.nodes):
        attractors = find_attractor(G)
        black_hole, pagerank = attractors[0]
        new_community = aggregate_nodes(G, black_hole, comms)
        comms[new_community.get_id()]= new_community
    nx.draw(G, with_labels= True)
    plt.show()
    #if nx.number_connected_components() is G.number_of_nodes():
     #   return com_map.values()


# return a graph with new nodes but old weights on edges
def aggregate_nodes(G, black_hole, communities):
    # type:(nx.DiGraph, str, dict)->(nx.DiGraph, Community)
    edges = list(G.in_edges([black_hole], data=True))
    edges.sort(key= __take_weight)
    src, dst, weight = edges.pop() # dst = black_hole
    retrieved = 0
    users = set()
    if communities.has_key(src):
        users= users.union(set(communities[src].users) )
        retrieved = retrieved+ 1
    if communities.has_key(dst):
        users= users.union(set(communities[dst].users ) )
        retrieved = retrieved + 1
    if retrieved < 2:
        clq2search = []
        if not communities.has_key(src):
            clq2search.append(src)
        if not communities.has_key(dst):
            clq2search.append(dst)
        cliques = clique_dao.get_cliques(clq2search)
        for c in cliques:
            users= users.union(set(c['nodes']) )
            retrieved = retrieved + 1

    try:
        if retrieved is 0: raise ValueError
        if retrieved is 1: raise ValueError
        if retrieved is 2: pass
        else : raise Exception
    except ValueError:
        print '0 clique retrieved'
        sys.exit(1)
    except Exception as e:
        print 'generic error in aggregate nodes', e
        sys.exit(1)
    # if has to aggregate communities-> implement fusion method in community
    community = Community(users, src, dst)
    #update G links and nodes
    nodes2remove = [src, dst]
    edges = [(s, d, data['weight']) for s, d, data in G.in_edges([src, dst], data= True)]
    G.remove_nodes_from(nodes2remove)
    if community.accept_in_links:
        edges2add = [(e[0], community.get_id(), e[2]) for e in edges]
        G.add_weighted_edges_from(edges2add)

    return community



def __take_weight(t):
    # type:(tuple)->float
    return t[2]['weight']