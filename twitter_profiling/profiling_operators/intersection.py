import itertools
import networkx as nx
import matplotlib.pyplot as plt


def rawIntersection(user_profiles):
    """ :type user_profiles: List of set """
    print('processing raw intersection on', len(user_profiles) )
    try:
        raw_intersection = set.intersection(*user_profiles)
        #print(raw_intersection)
        return raw_intersection
    except TypeError as e :
        print('input has to be a list of set', e)
    return None


def minus_k_intesection(user_profiles, k=1):
    profiles_number = len(user_profiles)
    print (profiles_number, 'passed profiles')
    user_combinations = list(itertools.combinations(user_profiles, profiles_number - k) )
    #print(user_profiles)
    print(user_combinations)
    profiles = []
    for combination in user_combinations:
        profiles.append(rawIntersection(combination) )
    p = set.union(*profiles)
    print('solution', p)
    return p


def minus_k_intersection_of_graph(user_graphs, k=1):
    #type:(list, int)->nx.Graph
    profiles_number = len(user_graphs)
    print (profiles_number, 'passed profiles')
    map_profiles = range(0, profiles_number)
    user_combinations = list(itertools.combinations(map_profiles, profiles_number - k))
    intersection_graphs = []
    for c in user_combinations:
        iteration_on_graphs = []
        for index in c:
            iteration_on_graphs.append(user_graphs[index])
        g = graph_intersection(iteration_on_graphs)
        intersection_graphs.append(g)
    G = __graph_union(intersection_graphs)
    return G

def graph_intersection(graphs):
    #type:(list[nx.Graph])->nx.Graph
    ##it is a binary operation
    #each I on 2 graphs -> each graphs have to be the same node set
    I = graphs.pop()
    for G in graphs:
        I.add_nodes_from(set(G.nodes).difference(set(I.nodes)) )
        G.add_nodes_from(set(I.nodes).difference(set(G.nodes)))
        I = nx.intersection(G, I)
    return I

def __graph_union(graphs):
    #type:(list[nx.Graph])->nx.Graph
    # U = graphs.pop()
    # for G in graphs:
    #     U.add_nodes_from(set(G.nodes).difference(set(U.nodes)))
    #     G.add_nodes_from(set(U.nodes).difference(set(G.nodes)))
    #     U = nx.union(G, U)
    U = nx.compose_all(graphs, 'clique_profile ')
    clean_U = nx.Graph()
    clean_U.add_edges_from(U.edges)
    return clean_U


#minus_kIntesection([{1,2,3,4},{2,4,5},{10,2,3,4,5,6},{10,2,3,4,5},{10,2,3,4,5,6,44}])
# G = nx.Graph()
# G.add_edges_from([(1,2),(2,3),(3,5),(1,5)])
# H = nx.Graph()
# H.add_edges_from([(1,2),(3,5),(1,5),(7,3)])
# J = nx.Graph()
# J.add_edges_from([(1,2),(3,6),(4,5),(7,5)])
# U = __graph_union([G, H, J])
# I = minus_k_intersection_of_graph([G,H,J])
# nx.draw(I, with_labels=True)
# plt.show()
