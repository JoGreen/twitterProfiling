from twitter_clique import clique_dao
from twitter_clique.cliques_graph_routines import generate
from twitter_graph.graph import UndirectedGraph#from twitter_profiling.clique_profiling.clique import Clique
from clique_profiling.clique import Clique
import matplotlib.pyplot as plt
import networkx as nx


cliques = clique_dao.get_limit_maximal_clique_on_specific_users_friendship_count(250, 1000)
cohesion_values = []
for c in cliques:
    clique = Clique(c['nodes'], str(c['_id']))
    chs = (clique.get_graph_cohesion() )
    cohesion_values.append(chs)
    if(chs<20):
        G = clique.get_knowledge_graph_from_users_kgraphs()
        nx.draw(G, with_labels= True)
        plt.show()

cohesion_values.sort()
min = cohesion_values[0]
max = cohesion_values[-1]
bins = (max -min)/20.0
plt.hist(cohesion_values, int(bins))
plt.show()