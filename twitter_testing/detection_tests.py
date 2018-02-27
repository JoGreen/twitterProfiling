import time
# from multiprocessing.pool import ThreadPool
from multiprocessing import Pool, cpu_count
from threading import Thread

import numpy as np

from persistance_mongo_layer.twitterdb_instance import DbInstance
from persistance_mongo_layer.dao.community_dao import get_all
from community_detection.clique_profiling.utility import constructor
from twitter_testing.profiling_metrics import profiles_overlapping, get_internal_mentions, get_common_mentions
from twitter_testing.topological_metrics import conductance, internal_density


#n_proc = {10:4,100:6,1000:8, 10000:12, 37000:20}


def statistics_on_iter(limit_dataset, iter):
    t0 = time.time()
    docs = get_all()
    comms = (constructor(doc) for doc in docs)
    cohesion_values =[]
    conductance_values = []
    internal_density_values = []
    profile_overlapping_percentages = []
    number_internal_mentions_per_user = []
    for c in comms:
        cohesion_values.append(c.get_vectors_cohesion() )
        conductance_values.append(conductance(c) )
        internal_density_values.append(internal_density(c) )
        profile_overlapping_percentages.append(profiles_overlapping(c))
        number_internal_mentions_per_user.append(get_internal_mentions(c)/float(len(c.users)) )
    cohesion_mean = np.array(cohesion_values).mean()
    internal_density_mean = np.array(internal_density_values).mean()
    conductance_mean = np.array(conductance_values).mean()
    profile_overlapping_mean = np.array(profile_overlapping_percentages).mean()
    mean_internal_mentions_per_user = np.array(number_internal_mentions_per_user).mean()


    file = open('log/statistics_'+str(limit_dataset)+'.txt', 'a')
    s1='iter'+str(iter)+'\n'
    s2 = 'cohesion_mean '+str(cohesion_mean)+', '
    s3 = 'internal_density_mean '+str(internal_density_mean)+', '
    s4 = 'conductance_values '+str(conductance_mean)+','
    s5 = 'profile_overlapping_mean '+str(profile_overlapping_mean)
    s6 = 'mean_internal_mentions_per_user '+ str(mean_internal_mentions_per_user)+'\n'
    file.write(s1+s2+s3+s4+s5+s6)
    file.close()
    print 'time: ', time.time() - t0


def do_statistics(limit_dataset, iter, n_before = None, n_after = None):
    #commented for analisys pourpose
    try:
        if n_before != None and n_after != None:
            compression = open('log/compression_performance' + str(limit_dataset) + '.txt', 'a')
            s = 'iter ' + str(iter) + '\n' + str(n_before) + ' -> ' + str(n_after) + '\n'
            compression.write(s)
            compression.close()
    except NameError: pass

    statistics_on_iter_multi_process(limit_dataset, iter)




res =[1,2,3,4,5]

def give_stat_on(doc):
    db = DbInstance().getDbInstance(new_client=True)
    #db = None
    com = constructor(doc)
    s1 = com.get_vectors_cohesion(db= db)
    s2 = (conductance(com, db=db))
    s3 = (internal_density(com, db=db))
    s4 = (profiles_overlapping(com, db=db))
    s5 = (get_internal_mentions(com, db=db) / float(len(com.users)))
    s6 =  get_common_mentions(com, db=db) #/ float(len(com.users))
    return s1, s2, s3, s4, s5, s6

def give_stat_on_thread(c):
    threads = []
    fs = [c_thread, id_thread, po_thread, im_thread, vc_thread]
    for f in fs:
        process = Thread(target= f, args=[c])
        process.start()
        threads.append(process)

    for p in threads:
        p.join()
    #s1=(c.get_vectors_cohesion(new_client= True))
    s1 = res[4] # c.get_vectors_cohesion()
    s2 =res[0] #(conductance(c))
    s3 =res[1] #(internal_density(c))
    s4 =res[2] #(profiles_overlapping(c))
    s5 =res[3] #(get_internal_mentions(c) / float(len(c.users)))
    #res = [1, 2, 3, 4]
    #print s1, s2, s3, s4, s5
    return s1, s2, s3, s4, s5


def vc_thread(c):
    #type:()->None
    val = c.get_vectors_cohesion()
    res[4] = val

def c_thread(c):
    val = conductance(c)
    res[0] = val

def id_thread(c):
    val = internal_density(c)
    res[1] = val

def po_thread(c):
    val = profiles_overlapping(c)
    res[2] = val

def im_thread(c):
    val = get_internal_mentions(c)
    res[3] = val


def statistics_on_iter_multi_process(limit_dataset, iter):
    t0 = time.time()

    docs = get_all()
        #comms = [constructor(doc) for doc in docs]
    pool = Pool(cpu_count()- 1)
    stat_values = pool.map(give_stat_on, docs)
    #stat_values = map(give_stat_on, docs)
    pool.close()
    docs.close()
    cohesion_values = [v[0] for v in stat_values]
    conductance_values = [v[1] for v in stat_values]
    internal_density_values = [v[2] for v in stat_values]
    profile_overlapping_percentages = [v[3] for v in stat_values]
    number_internal_mentions_per_user = [v[4] for v in stat_values]
    number_common_out_mentions_per_user = [v[5] for v in stat_values]

    cohesion_mean = np.array(cohesion_values).mean()
    internal_density_mean = np.array(internal_density_values).mean()
    conductance_mean = np.array(conductance_values).mean()
    profile_overlapping_mean = np.array(profile_overlapping_percentages).mean()
    mean_internal_mentions_per_user = np.array(number_internal_mentions_per_user).mean()
    mean_out_common_mentions_per_user = np.array(number_common_out_mentions_per_user).mean()

    file = open('log/statistics_' + str(limit_dataset) + '.txt', 'a')
    s1 = 'iter' + str(iter) + '\n'
    s2 = 'cohesion_mean ' + str(cohesion_mean) + ', '
    s3 = 'internal_density_mean ' + str(internal_density_mean) + ', '
    s4 = 'conductance_values ' + str(conductance_mean) + ','
    s5 = 'profile_overlapping_mean ' + str(profile_overlapping_mean)+ ', '
    s6 = 'mean_internal_mentions_per_user ' + str(mean_internal_mentions_per_user) + ', '
    s7 = 'mean_common_out_mentions_per_user ' + str(mean_out_common_mentions_per_user) + '\n' #common for 90%users
    file.write(s1 + s2 + s3 + s4 + s5 + s6+ s7)
    file.close()
    print 'time: ', time.time() - t0
