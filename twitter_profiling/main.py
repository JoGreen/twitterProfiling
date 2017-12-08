from twitter_clique import clique_dao
from twitter_profiling.community.dao.community_dao import do_dump
# from clique_profiling.clique import Clique
from clique_profiling.utility import constructor, destroy_useless_clique
# from twitter_profiling.clique_profiling.clique_graph import neighbour_graph
from twitter_profiling.clique_profiling.routines import aggregation_step
import matplotlib.pyplot as plt, itertools

#from multiprocessing import Pool
import os, time, math, numpy as np
from twitter_profiling.clique_clustering.density import dbscan_on_clique_neighbourhood
from twitter_profiling.clique_profiling.clique import Clique
from twitter_profiling.community.dao.community_dao import get_count
from twitter_testing.detection_tests import do_statistics
from twitter_profiling.user_profiling.profile_dao import ProfileDao

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

def inizializing(restart):
    # type:(bool)->(set([str]), set([str]) )
    try:
        if restart == False:
            f_visited = open("visited.txt", "r")
            data_v = f_visited.read()
            f_visited.close()
            if data_v == '': raise IOError
            visited = data_v.split(',')
            # visited.pop() #delete last elem-> it s an empty string

            f_deleted = open("deleted_ids.txt", "r")
            data_d = f_deleted.read()
            f_deleted.close()
            if data_d == '': raise IOError
            deleted = data_d.split(',')
            # deleted.pop()  # delete last elem-> it s an empty string

            print 'initializing phase done.'
            print len(visited), 'visited ids'
            return set(visited), set(deleted)
        else:
            f_not_computed = open('not_computed.txt', 'w')
            f_not_computed.close()
            f_deleted = open('deleted_ids.txt', 'w')
            f_deleted.close()
            f_time_log= open('time_log.txt', 'w')
            f_time_log.close()
            return set(), set()
    except IOError :
        #destroy_useless_clique()
        print 'at least one of visited.txt file or deleted_ids.txt not exists or it is empty. computation is starting from scratch'
        f_deleted = open('deleted_ids.txt', 'w')
        f_deleted.close()
        f_not_computed = open('not_computed.txt', 'w')
        f_not_computed.close()
        return set(), set()

def update_visited_file(visited):
    visited_file = open('visited.txt', 'w')
    string_to_write = ",".join(visited)
    visited_file.write(string_to_write)
    visited_file.close()

datasets = [37000]

#limit_dataset = 100
#clique_dao.create_dataset(limit_dataset)
minimum_num_of_interests = Clique.minimum_num_of_interests

def iteration(limit_dataset, restart = False):
    visited, deleted = inizializing(restart)
    # cliques = clique_dao.get_limit_maximal_cliques_on_valid_users(10)
    cliques = clique_dao.get_maximal_cliques()#.skip(5000)

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
    n = 37000 #huge number
    iter = 0
    do_statistics(limit_dataset, iter)
    while get_count() < n:
        n = get_count()
        iter = iter + 1
        iteration(limit_dataset, restart=True)
        if get_count() < n:
            do_statistics(limit_dataset, iter, n)
            __rename_files(limit_dataset, iter)
            do_dump(limit_dataset, iter)


def __rename_files(limit_dataset, iter):
    f1 = 'visited.txt'
    f2= 'not_computed.txt'
    f3 = 'time_log.txt'
    s = str(limit_dataset)+'_iter'+str(iter)+'_'
    os.rename(f1, 'log/'+s+ f1)
    os.rename(f2, 'log/'+s+ f2)
    os.rename(f3, 'log/'+s+ f3)



def create_folders():
    try:
        os.mkdir('log')
    except OSError:
        print 'log folder already exists.'
    try:
        os.mkdir('dumps')
    except OSError:
        print 'dumps folder already exists.'


#ProfileDao().get_all_useful_profiles(cache= True)

create_folders()
map(cycle, datasets)

