import numpy as np

from persistance_mongo_layer.dao import clique_dao
from persistance_mongo_layer.dao.profile_dao import ProfileDao


def mean_interest_per_user():
    docs = ProfileDao().get_all_useful_profiles()
    num_of_interests = [len(doc['info']['interests']['all'].keys() ) for doc in docs ]

    return np.array(num_of_interests).mean()

def total_users():
   clqs =  clique_dao.get_maximal_cliques()
   users_per_clique = [set(clq['nodes']) for clq in clqs ]
   total = len(set.union(*users_per_clique) )
   return total

def total_users_per_dataset():
    clique_dao.create_dataset(37000)
    print total_users(), 'users in 37000 cliques '
    clique_dao.create_dataset(10000)
    print total_users(), 'users in 10000 cliques '
    clique_dao.create_dataset(1000)
    print total_users(), 'users in 1000 cliques '
    clique_dao.create_dataset(100)
    print total_users(), 'users in 100 cliques '
    clique_dao.create_dataset(10)
    print total_users(), 'users in 10 cliques '
