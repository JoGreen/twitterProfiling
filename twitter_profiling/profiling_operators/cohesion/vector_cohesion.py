import itertools, numpy as np
from twitter_profiling.profiling_operators.similarity import vector_distance, vector_distance_between_all
from twitter_profiling.profiling_operators.means import geometric_mean

#from multiprocessing import Pool

def cosine_cohesion_distance(vectors):
    # type:(list[list[int]])->float
    # pairs = [i for i in itertools.combinations(vectors, 2) ]
    vectors = map(__check_for_zero_vector, vectors)
    pairs = itertools.combinations(vectors, 2)

    cohesion_values = [vector_distance(p) for p in pairs]
    # cohesion_values = np.array(cohesion_values)

    # pool = Pool(len(pairs))
    # cohesion_values = pool.map(vector_distance, pairs)
    # do normalization on number of dimensions of clique space ? maybe not because angle is not influenced by the dimensions
    return geometric_mean(cohesion_values)


def cosine_cohesion_distance_optimized(vectors):
    vectors = map(__check_for_zero_vector, vectors)
    cohesion_values = vector_distance_between_all(vectors)
    return geometric_mean(cohesion_values)

def __check_for_zero_vector(v):
    # type: (list[int])->list[float]
    try:
        list(v)
    except TypeError: print v, type(v)
    if len(v) > 0:
        sv = set(v) # use as et because i see if it s composed by only  0s
        if len(sv) == 1 and sv.pop() == 0:
            v[0] = v[0] + 0.00001
        return v




#v1 = [1,2,3,4,5,6]
#v2 = [1,7,8,6,66,2]

#cosine_cohesion_distance([v1,v2])