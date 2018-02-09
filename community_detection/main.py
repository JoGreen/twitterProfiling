import math
import os
import time
from multiprocessing import Pool

import matplotlib.pyplot as plt
import numpy as np

# from clique_profiling.clique import Clique
from clique_profiling.utility import constructor, destroy_useless_clique
from persistance_mongo_layer.dao import clique_dao
from persistance_mongo_layer.dao.community_dao import do_dump, do_restore
from persistance_mongo_layer.dao.community_dao import get_count as get_com_count
from community_detection.clique_clustering.density import dbscan_on_clique_neighbourhood
from community_detection.clique_profiling.clique import Clique

from community_detection.routines import aggregation_step
from twitter_testing.detection_tests import do_statistics

from help import inizializing, update_visited_file, create_folders, rename_files


def cohesion(c):
    clique = constructor(c)#Clique(c['nodes'], str(c['_id']))
    # chs = (clique.get_graph_cohesion())
    chs = (clique.get_vectors_cohesion() )
    return chs

def multithread(numb_proc):
    # type:(int)->list[float]
    #cliques = clique_dao.get_limit_maximal_clique_on_specific_users_friendship_count(250, 2000)
    cliques= clique_dao.get_limit_maximal_cliques_on_valid_users(1000) #limit parameter
    print __name__
    if __name__ == 'community_detection.main':
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
            clq = constructor(c)# Clique(c['nodes'], c['_id'])
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





datasets = [10, 100, 1000]

#limit_dataset = 100
#clique_dao.create_dataset(limit_dataset)
minimum_num_of_interests = Clique.minimum_num_of_interests

def iteration(limit_dataset, restart = False):
    visited, deleted = inizializing(restart)

    cliques = clique_dao.get_maximal_cliques()

    t_start = time.time()

    for index, c in enumerate(cliques):
        clq = constructor(c) # return a clique or a community # Clique(c['nodes'], c['_id'])

        if not clq.get_id() in visited and not clq.get_id() in deleted and \
                len(clq.get_profile() ) > minimum_num_of_interests and \
                clq.enough_cohesion():

            t0= time.time()
            print 'step n.', index
            G, visited, new_deleted = aggregation_step(clq, visited)
            deleted.union(new_deleted)
            #print 'visited', len(visited), 'nodes'
            update_visited_file(visited)
            t1 = time.time()
            log = 'step '+ str(index)+ ' in time: '+ str(t1-t0)+'\n'
            time_log= open('time_log.txt', "a")
            time_log.write(log)
            time_log.close()
        else:
            # print 'not computed'
            if len(clq.get_profile() ) <= minimum_num_of_interests or not clq.enough_cohesion():
                f_not_computed = open('not_computed.txt', 'a')
                f_not_computed.write(clq.get_id() + ',')
                f_not_computed.close()


    print 'the end.'
    t_end = time.time()
    time_log = open('time_log.txt', "a")
    log = 'computation ended in '+ str(t_end-t_start)+'\n'+'dataset of '+str(limit_dataset)
    time_log.write(log)
    time_log.close()
    cliques.close()


def cycle(limit_dataset):
    clique_dao.create_dataset(limit_dataset)
    destroy_useless_clique()
    #n = 37000 #huge number
    n_before_iter = max(datasets) + 1
    iter = 0
    do_statistics(limit_dataset, iter)
    while get_com_count() < n_before_iter:
        n_before_iter = get_com_count()
        iter = iter + 1
        iteration(limit_dataset, restart=True)
        n_after_iter = get_com_count()
        if get_com_count() < n_before_iter:
            do_statistics(limit_dataset, iter, n_before_iter, n_after_iter )
            rename_files(limit_dataset, iter)
            do_dump(limit_dataset, iter)









def cycle_dump_for_stat(limit_dataset):
    #####clique_dao.create_dataset(limit_dataset)
    #do_restore(limit_dataset)
    #####destroy_useless_clique()
    n = 37000  # huge number
    iter = 0
    #do_statistics(limit_dataset, iter)
    thereis_file = True
    iter = iter+1
    while thereis_file:
        try:
            f = open('dumps/dump_' + str(limit_dataset) + '_iter' + str(iter)+ '/twitter/comms.bson', 'r')
            f.close()
            do_restore(limit_dataset, iter)
            do_statistics(limit_dataset, iter)
            iter = iter + 1
        except IOError:
            thereis_file = False

#ProfileDao().get_all_useful_profiles(cache= True)

create_folders()


map(cycle, datasets)
#map(cycle_dump_for_stat, datasets)