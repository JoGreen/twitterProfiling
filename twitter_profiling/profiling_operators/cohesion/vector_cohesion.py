import itertools
from twitter_profiling.profiling_operators.similarity import vector_distance
from twitter_profiling.profiling_operators.means import geometric_mean
from multiprocessing import Pool

def cosine_cohesion(vectors):
    # type:(list[list[int]])->float
    # pairs = [i for i in itertools.combinations(vectors, 2) ]
    pairs = itertools.combinations(vectors, 2)
    cohesion_values = [vector_distance(p) for p in pairs]
    # pool = Pool(len(pairs))
    # cohesion_values = pool.map(vector_distance, pairs)
    # do normalization on number of dimensions of clique space ? maybe not because angle is not influenced by the dimensions
    return geometric_mean(cohesion_values)


