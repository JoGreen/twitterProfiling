import networkx as nx
import itertools
from numpy import median, std, mean
from twitter_profiling.profiling_operators.means import geometric_mean
from twitter_profiling.profiling_operators.similarity import isomorphism_measure
from twitter_graph.graph import UndirectedGraph, weighted_graph_mapping


def cohesion_of_graph_profiles(graphs):
    # type:(list[nx.Graph])->float
    try:
        graphs = list(graphs)
    except TypeError:
        print 'input of cohesion_of_graph_profiles has to be a list of nx.Graph'
    __normalize(graphs)  # normalize weights
    map = range(0, len(graphs))
    number_nodes = graphs[0].number_of_nodes() #should have all the same # of nodes
    combinations = itertools.combinations(map, 2)
    isomorphism_values = []
    for c in combinations:
        val = isomorphism_measure(
            weighted_graph_mapping(graphs[c[0]]),
            weighted_graph_mapping(graphs[c[1]])
        )
        isomorphism_values.append(val/float(number_nodes))

    print isomorphism_values
    print 'geometric mean', geometric_mean(isomorphism_values)
    print 'median value', median(isomorphism_values)
    print 'dev standard', std(isomorphism_values)
    print 'mean', mean(isomorphism_values)

    return geometric_mean(isomorphism_values)


def __normalize(graphs):
    # type:(list(nx.Graph))->list(nx.Graph)

    map = {}
    for g in graphs:
        for e in g.edges('User', data=True):

            user_index = e.index('User')
            if user_index == 0:
                interest_index = 1
            else:
                interest_index = 1
            interest = e[interest_index]
            if not interest in map.values():
                map[interest] = e[2]['weight']
            else:
                map[interest] = max(map[interest], e[2]['weight'])

    print map
    for g in graphs:
        for e in g.edges('User', data=True):

            user_index = e.index('User')
            if user_index == 0:
                interest_index = 1
            else:
                interest_index = 1
            interest = e[interest_index]

            if map[interest] > 10:
                weight = e[2]['weight']
                new_weight = (weight / map[interest]) * 10
                g[e[0]][e[1]]['weight'] = new_weight
