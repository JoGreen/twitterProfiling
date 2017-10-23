import networkx as nx
import matplotlib.pyplot as plt
import itertools

class UndirectedGraph:
    #links = [] # list of tuples

    def __init__(self):
        self.graph = nx.Graph()


    def get_graph(self):
        return self.graph

    def set_graph(self, graph):
        #type:(UndirectedGraph, nx.Graph)->None
        self.graph = graph

    def add_link(self, n1, n2, weight = 1):
        self.graph.add_edge(n1, n2, weight=weight)

    def add_links(self, links):
        links = list(links)
        self.graph.add_edges_from(links)

    def draw(self): #draw a graph without anything more. es. no label
        nx.draw(self.graph)
        plt.show()

    def draw_with_labels(self, labels_mapping=None):
        if labels_mapping is None:
            nx.draw(self.graph, with_labels= True)
        else:
            pass #nx.draw_networkx_labels(self.graph, labels= labels_mapping, with_labels= True, arrows= False, pos=nx.spring_layout(self.graph))
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
    #type:(nx.Graph)->set
    n_links= len(G.edges)
    if n_links < 2 and len(G.nodes) > 0:
        return set(G.nodes[0])
    else:
        for vertex_cardinality in range(2, len(G.nodes) ):
            combo = itertools.combinations(set(G.nodes), vertex_cardinality)
            for c in combo:
                if __is_valid_vertex(c, G) is True: return c
    print 'not found vc -> it s impossible => human mistake'
    return None


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

def weighted_graph_mapping(G):
    # type: (nx.Graph)->nx.Graph
    for e in G.edges:
        source = e[0]
        target = e[1]
        weight = G[source][target]['weight']
        if weight is not 1:
            G.remove_edge(source, target)
            G.add_edges_from([(source, source+target),(target, source+target)])
            w
    pass