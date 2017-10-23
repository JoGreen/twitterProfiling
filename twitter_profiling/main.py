from twitter_clique import clique_dao
from twitter_clique.cliques_graph_routines import generate
from twitter_graph.graph import UndirectedGraph


cliques = clique_dao.get_limit_maximal_clique_on_specific_users_friendship_count(150, 10)
g = generate(cliques)
print 'degree of graph',
degree_histogram = g.degree_histogram()

print (degree_histogram)

components = g.connected_components()
#components.count()
#common_nodes = set(components[0]).intersection(set(components).remove(components[0]))
#print(common_nodes)
for c in components:
    print c
    print(len(c))

g.draw()