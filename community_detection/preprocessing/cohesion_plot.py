from multiprocessing import Pool, cpu_count
from community_detection.clique_profiling.utility import constructor
from persistance_mongo_layer.dao import clique_dao
from community_detection.profiling_operators.means import geometric_mean
from matplotlib import pyplot as plt
import time, math
import numpy as np

def cohesion(c):
    clique = constructor(c)#Clique(c['nodes'], str(c['_id']))
    chs = clique.get_vectors_cohesion()
    #chs = clique.get_vectors_cohesion(db=DbInstance().getDbInstance(new_client= True) )
    return chs

def compute_cohesion_values_multiproc():
    # type:(int)->list[float]
    #cliques = clique_dao.get_limit_maximal_clique_on_specific_users_friendship_count(250, 2000)
    cliques= clique_dao.get_limit_maximal_cliques_on_valid_users(30000, gt=1) #limit parameter

    pool = Pool(cpu_count()- 1)

    cohesion_values = pool.map(cohesion, cliques)
    pool.close()

    # cohesion_values = []
    # for c in cliques:
    #     # clique = Clique(c['nodes'], str(c['_id']))
    #     chs = cohesion(c)
    #     cohesion_values.append(chs)

    cliques.close()
    return cohesion_values



def compute_plot_clique_cohesion():
    t0 = time.time()
    cohesion_values = compute_cohesion_values_multiproc()
    t1 = time.time()

    print 'time execution', 'thread=', t1- t0
    ####compute avg
    cohesion_avg = np.mean(cohesion_values)
    print 'cohesion avg = ', cohesion_avg
    cohesion_geom_mean = geometric_mean(cohesion_values)
    print 'geometric mean of cohesion values = ', cohesion_geom_mean
    cohesion_variance = np.var(cohesion_values)
    print 'variance is = ', cohesion_variance
    ###
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


compute_plot_clique_cohesion()