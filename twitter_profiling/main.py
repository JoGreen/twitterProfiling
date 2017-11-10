from twitter_clique import clique_dao

from clique_profiling.clique import Clique
from twitter_profiling.clique_profiling.clique_graph import neighbour_graph
from twitter_profiling.clique_profiling.routines import expand
import matplotlib.pyplot as plt
import networkx as nx
from multiprocessing import Pool
import os, time, math, numpy as np
from twitter_profiling.clique_clustering.density import dbscan_on_clique_neighbourhood


def cohesion(c):
    clique = Clique(c['nodes'], str(c['_id']))
    # chs = (clique.get_graph_cohesion())
    chs = (clique.get_vectors_cohesion() )
    return chs

def multithread(numb_proc):
    # type:(int)->list[float]
    #cliques = clique_dao.get_limit_maximal_clique_on_specific_users_friendship_count(250, 2000)
    cliques= clique_dao.get_limit_maximal_cliques_on_valid_users(1000) #limit parameter
    print __name__
    if __name__ == 'twitter_profiling.main':
        pool = Pool(numb_proc)
        # cohesion_values = []
        clqs = [c for c in cliques]
        print len(clqs), 'prima del multiprocess'
        cliques.close()
        # # cohesion_values = [pool.apply_async(os.getpid, ()) for c in cliques]
        cohesion_values = pool.map(cohesion, clqs)


    # for c in cliques:
    #     # clique = Clique(c['nodes'], str(c['_id']))
    #     chs = cohesion(c)
    #     cohesion_values.append(chs)

        return cohesion_values


#####cluster membership
def cluster_membership():
    cliques = clique_dao.get_limit_maximal_cliques_on_valid_users(200)
    i = -1
    for c in cliques:
        i = i+ 1
        if i is 15:
            clq = Clique(c['nodes'], c['_id'])
            cluster_memberships = dbscan_on_clique_neighbourhood(clq)
            print cluster_memberships


####to plot cohesion values
def compute_plot_clique_cohesion():
    proc = 9
    t0 = time.time()
    cohesion_values = multithread(proc)
    t1 = time.time()

    print 'time execution',proc, 'thread=', t1- t0

    cohesion_values.sort()
    min = cohesion_values[0]
    max = cohesion_values[-1]
    bins = 50
    tick_frequency = (max-min)/ bins
    if tick_frequency < 1:
        tick_frequency = (10**(int(math.log10(abs(tick_frequency)))-1) )*5
        print int(math.log10(abs(tick_frequency))) *(-1)+1
    print 'tick frequency', tick_frequency, 'min', min,'max', max
    plt.hist(cohesion_values, int(bins))
    plt.xticks(np.arange(0, max, tick_frequency))
    plt.show()
###########################################
def step():
    cliques = clique_dao.get_limit_maximal_cliques_on_valid_users(10)
    i= -1
    for c in cliques:
        i = i+ 1
        if i is 2:
            clq = Clique(c['nodes'], c['_id'])
            expand(clq)
            # neighbour_graph(clq)



step()