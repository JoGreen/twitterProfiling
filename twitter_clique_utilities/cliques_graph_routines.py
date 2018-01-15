from numpy import mean
from numpy import median
from numpy import std
from pymongo.cursor import Cursor

from trash.twitter_graph import UndirectedGraph
from community_detection.clique_profiling.clique import Clique
from community_detection.profiling_operators.cohesion.set_cohesion import profiles_cohesion
from persistance_mongo_layer.dao.clique_dao import get_similar_cliques_on_nodes


def generate(cliques): #confusional
    #type:(Cursor)->UndirectedGraph
    i = 0
    g = UndirectedGraph()
    for clique in cliques:  # clique as in cliques2analyze Mongo -> it s a dict
        i = i+1
        print('step', i)
        print('clique:', str(clique['_id']))
        neighbourhood_candidates = __the_closers(clique)
        clq = Clique(clique['nodes'], clique['_id'])
        # print 'is a clique', clq.is_a_clique()
        clq_cohesion = profiles_cohesion((clq.get_users_profiles() ) )
        print('clique cohesion =', clq_cohesion)

        # compute similarity on profiles
        similarity_values = [clq.profile_set_similarity_with(Clique(c['nodes'], c['_id'])) for c in
                             neighbourhood_candidates]
        for index_sim in range(0,len(similarity_values) ):
            if similarity_values[index_sim] < 0.41: print clq.get_id() , str(neighbourhood_candidates[index_sim]["_id"]), 'low similarityy  aiai why'

        # statistics on similarity values
        threeshold = median(similarity_values)  # good if values are not so close
        media = mean(similarity_values)
        stdev = std(similarity_values)
        threeshold2 = media - stdev

        print(similarity_values)
        print('mediana, media - deviazione standard:'),
        print(threeshold, threeshold2)
        threeshold_list = [v for v in similarity_values if v >= threeshold]
        threeshold2_list = [v for v in similarity_values if v >= threeshold2]

        print('clique aventi similarita > della mediana, della media - deviazione standard, tutte'),
        print(len(threeshold_list), len(threeshold2_list), len(similarity_values))
        print

        for neighbour in neighbourhood_candidates:
            g.add_link( str(neighbour['_id']), clq.get_id() )

    cliques.close()
    return g


def __the_closers(clique, k=1): #move to Clique class ?
    return get_similar_cliques_on_nodes(clique, k)  # clique as in cliques2analyze




# from twitter_clique_utilities import clique_dao
#
# cliques = clique_dao.get_limit_maximal_cliques(300, 8)
# g = generate(cliques)
# components = g.connected_components()
# #components.count()
# for c in components:
#     print c

