import networkx as nx
import itertools
from numpy import median, std, mean
from twitter_profiling.profiling_operators.means import geometric_mean
from twitter_profiling.profiling_operators.similarity import isomorphism_measure
from twitter_graph.graph import UndirectedGraph, weighted_graph_mapping

def cohesion_of_graph_profiles(graphs):
    #type:(list(nx.Graph))->int
    try:
        graphs = list(graphs)
    except TypeError : print 'input of cohesion_of_graph_profiles has to be a list of nx.Graph'
    map = range(0, len(graphs) )
    combinations = itertools.combinations(map, 2)
    isomorphism_values = []
    for c in combinations:
        isomorphism_values.append(isomorphism_measure(weighted_graph_mapping(graphs[c[0]]),
                                                      weighted_graph_mapping(graphs[c[1]]) ) )

    print isomorphism_values
    print 'geometric mean', geometric_mean(isomorphism_values)
    print 'median value', median(isomorphism_values)
    print 'dev standard', std(isomorphism_values)
    print 'mean', mean(isomorphism_values)

    return median(isomorphism_values)