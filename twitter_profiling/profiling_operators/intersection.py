import itertools, sys
import networkx as nx
import numpy as np
from functools import reduce
import matplotlib.pyplot as plt
from multiprocessing import Pool
import time

def rawIntersection(user_profiles):
    # type:(list[set])->set
    # print('processing raw intersection on', len(user_profiles) )
    try:
        #user_profiles = list(user_profiles)
        raw_intersection = set.intersection(*user_profiles)
        #raw_intersection = reduce(np.intersect1d, (user_profiles ))
        #print(raw_intersection)
        return raw_intersection
    except TypeError as e :
        print('input has to be a list of set', e)
        sys.exit(1)


def minus_k_intersection(user_profiles, k=1):
    t0= time.time()
    profiles_number = len(user_profiles)

    user_combinations = itertools.combinations(user_profiles, profiles_number - k)
    #user_combinations = itertools.combinations(user_profiles, profiles_number - k)
    #print len(user_combinations)
    # t0 = time.time()
    # pool = Pool(10)
    # profiles = pool.map(rawIntersection, user_combinations)
    # pool.close()
    # t1 = time.time()
    # print t1 - t0

    profiles = map(rawIntersection, user_combinations)
    # profiles = filter(lambda i : i != None, profiles) #is ut useful ?
    p = set.union(*profiles)
    print 'k = ', k
    print 'minus_k_intersection using union of intersection of all combinations of length #nodes-k: ',time.time()-t0
    print 'profile solution built using k-intersection'
    print
    print p
    return p

def minus_k_intersection_linear(user_profiles, k=1):
    #t0 = time.time()
    threeshold = len(user_profiles) - k
    count_map = {}
    for up in user_profiles:
        for interest in up:
            try:
                count_map[interest] = count_map[interest] + 1
            except KeyError:
                count_map[interest] = 1

    intersection = [key for key, count in count_map.items() if count >= threeshold]
    #print 'k = ', k
    return intersection

def numpy_minus_k_intersection(user_profiles, k=1):# really slow
    t0 = time.time()
    profiles_number = len(user_profiles)
    all_interests = list(set().union(*user_profiles))
    user_profiles_mapped = (map(all_interests.index, up)  for up in user_profiles)

    user_combinations_mapped = itertools.combinations(user_profiles_mapped, profiles_number - k)
    profiles = map(numpy_rawIntersection, user_combinations_mapped)
    p = reduce(np.union1d, (profiles))
    p = map(lambda x: demapping(x, all_interests), p)
    print time.time() - t0
    print p
    return p

def numpy_rawIntersection(user_profiles):
    # type:(list[set])->set
    # print('processing raw intersection on', len(user_profiles) )
    try:
        user_profiles = list(user_profiles)
        #raw_intersection = set.intersection(*user_profiles)
        raw_intersection = reduce(np.intersect1d, (user_profiles ))
        #print(raw_intersection)
        return raw_intersection
    except TypeError as e :
        print('input has to be a list of set', e)
        sys.exit(1)


def __launch_combinations(slot):
    return itertools.combinations(slot, len(slot) - 1)

def minus_k_intersection_of_graph(user_graphs, k=1):
    #type:(list, int)->nx.Graph
    profiles_number = len(user_graphs)
    print (profiles_number, 'passed profiles')
    map_profiles = range(0, profiles_number)
    user_combinations = list(itertools.combinations(map_profiles, profiles_number - k))
    graph_combiantions = []

    # for c in user_combinations:
    #     iteration_on_graphs = []
    #     for index in c:
    #         iteration_on_graphs.append(user_graphs[index])
    #     graph_combiantions.append(iteration_on_graphs)
    intersection_graphs = []

    # pool = Pool(len(user_combinations) )
    # intersection_graphs = pool.map(graph_intersection(user_graphs), graph_combiantions)

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



def minus_k_intesection_optimized(user_profiles, k=1):
    profiles_number = len(user_profiles)
    print (profiles_number, 'passed profiles')
    user_combinations = list(itertools.combinations(user_profiles, profiles_number - k) )

    pool = Pool(len(user_combinations))
    profiles = pool.map(rawIntersection, user_combinations)
    # profiles = []

    # for combination in user_combinations:
    #     i = rawIntersection(combination)
    #     if i is not None:
    #         profiles.append(i)

    p = set.union(*profiles)
    print('solution', p)
    return p


def demapping(el, mapp):
    return mapp[el]



    # if profiles_number> 21 or k > 5:
    #     slots = np.array_split(np.array(list(user_profiles)), k)
    #     slots = map(set, slots)
    #     user_combinations = map(__launch_combinations, slots)


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
