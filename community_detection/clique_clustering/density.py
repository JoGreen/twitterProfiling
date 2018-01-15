from sklearn.cluster import DBSCAN
from community_detection.clique_profiling.clique import Clique

def dbscan_on_clique_neighbourhood(clq):
    # type:(Clique)->dict
    neighb = clq.get_neighbours(k=1)
    #neighb = clq.get_the_closers(k=2)
    print 'number of neighbour', len(neighb)
    neighb.append(clq)
    neighb_profile_weights = [clique.get_weighted_profile() for clique in neighb]

    interests = set()
    for npw in neighb_profile_weights:  # it s a dict
        interests = interests.union(set(npw.keys()))
    print 'number of interests =', len(interests)

    for npw in neighb_profile_weights:  # it s a dict
        for i in interests:
            if not i in npw:
                npw[i] = 0.


    values = [weight.values() for weight in neighb_profile_weights]
    labels = DBSCAN(eps=0.00001, min_samples=2, metric='cosine').fit_predict(values)
    print (set(labels))
    map_of_cluster = {}
    for index, l in enumerate(labels):
        clq_id = neighb[index].get_id()
        map_of_cluster[clq_id] = l
    return  map_of_cluster