from twitter_profiling.community.dao.community_dao import get_all, get_count
from twitter_profiling.clique_profiling.utility import constructor
import numpy as np
from twitter_profiling.profiling_operators.cohesion.topological_metrics import conductance, internal_density
from twitter_testing.profiling_metrics import profiles_overlapping, get_internal_mentions

def statistics_on_iter(limit_dataset, iter):
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



def do_statistics(limit_dataset, iter, n = None):
    try:
        compression = open('log/compression_performance' + str(limit_dataset) + '.txt', 'a')
        s = 'iter ' + str(iter) + '\n' + str(n) + ' -> ' + str(get_count()) + '\n'
        compression.write(s)
        compression.close()
    except NameError: pass
    statistics_on_iter(limit_dataset, iter)



# not used
def mean_cohesion():
    docs = get_all()
    comms = map(constructor, docs)
    values = [ c.get_vectors_cohesion() for c in comms]
    values = np.array(values)
    value = values.mean()
    return value


