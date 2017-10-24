import networkx as nx
import matplotlib.pyplot as plt
import itertools
import sys


class UndirectedGraph:
    # links = [] # list of tuples

    def __init__(self):
        self.graph = nx.Graph()

    def get_graph(self):
        return self.graph

    def set_graph(self, graph):
        # type:(UndirectedGraph, nx.Graph)->None
        self.graph = graph

    def add_link(self, n1, n2, weight=1):
        self.graph.add_edge(n1, n2, weight=weight)

    def add_links(self, links):
        links = list(links)
        self.graph.add_edges_from(links)

    def draw(self):  # draw a graph without anything more. es. no label
        nx.draw(self.graph)
        plt.show()

    def draw_with_labels(self, labels_mapping=None):
        if labels_mapping is None:
            nx.draw(self.graph, with_labels=True)
        else:
            pass  # nx.draw_networkx_labels(self.graph, labels= labels_mapping, with_labels= True, arrows= False, pos=nx.spring_layout(self.graph))
        plt.show()

    def connected_components(self):
        components = nx.connected_components(self.graph)
        return components

    def degree_histogram(self):
        return nx.degree_histogram(self.graph)


class DiGraph(UndirectedGraph):
    def __init__(self):
        self.graph = nx.DiGraph()


def minimum_vertex_cover(G):
    # type:(nx.Graph)->set
    n_links = len(G.edges)
    if n_links < 2 and len(G.nodes) > 0:
        return set(G.nodes[0])
    else:
        for vertex_cardinality in range(2, len(G.nodes)):
            combo = itertools.combinations(set(G.nodes), vertex_cardinality)
            for c in combo:
                if __is_valid_vertex(c, G) is True: return c
    print 'not found vc -> it s impossible => human mistake'
    return None

def dense_minimum_vertex_cover(G):
    #type:(nx.Graph)->int
    print type(G)
    if len(G.edges) < 2 and len(G.nodes) > 0:
        return 1

    #nodes = list(G.degree().values() ).sort(reverse=True)
    degree_tuples_node_degree = sorted(list(G.degree()), key= lambda t:t[1], reverse=True ) # sort by degree
    ordered_nodes = [n[0] for n in degree_tuples_node_degree]
    #use degree to improve performance on big graph
    reverse_range = range(2, len(ordered_nodes), 1)
    reverse_range.sort(reverse= True)
    for vertex_cardinality in reverse_range:
        combo = itertools.combinations(set(ordered_nodes), vertex_cardinality)
        found = False
        #for i in xrange(0, int(len(combo)*2.0/3.0), 3 ):
        for c in combo:
            found = found or __is_valid_vertex(c, G) is True
        if found is False and vertex_cardinality < G.number_of_nodes():
            return vertex_cardinality + 1
    print 'not found vc -> it s impossible => human mistake'
    return None


def dense_connected_components_minimum_vertex_cover(G):
    # type: (nx.Graph)->int

    vc = 0
    for C in list(nx.connected_component_subgraphs(G) ) :
        try:
            vc = vc+ dense_minimum_vertex_cover(C)
        except TypeError:
            print 'ERROR in vertex cover cardnality of a components.. it is None'
            sys.exit(1)
    return vc

def __is_valid_vertex(vertex, G):
    for e in G.edges:
        contains = False
        for v in vertex:
            if v in e:
                contains = True
                break
        is_vertex = True and contains
        if is_vertex is False: return is_vertex
    return is_vertex


def weighted_graph_mapping(WG): #something wrong
    # type: (nx.Graph)->nx.Graph

    edges2delete = []
    edges2append = []
    G = WG.copy()
    print len(WG.edges), 'edges'
    for e in WG.edges:
        source = str(e[0])
        target = str(e[1])

        try:
            weight = WG.get_edge_data(*e)['weight']
        except KeyError:
            weight = 1

        if weight > 1 and weight is not None:
            edges2delete.append(e)
            edges2append.extend([(source, source + target + str(1)), (target, source + target + str(1))])
            nodes = [source + target + str(i) for i in range(1, weight/10)]
            cliques_links = [tuple(l) for l in itertools.combinations(nodes, 2)]  # generate a clique of weight-1 dimension
            edges2append.extend(cliques_links)
    G.remove_edges_from(edges2delete)
    G.add_edges_from(edges2append)
    print 'nodes after reduction', len(G.nodes)
    #G = nx.compose_all(cliques, 'reduction_graph')
    return G
