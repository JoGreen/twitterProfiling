from pyspark.sql import SparkSession
import operator
max_depth = 2

def spark_neighbour_graph(clq, lev=1):
    # type:(Clique)->nx.DiGraph
   # neighb_and_cohesion = clq.get_neighbours(k=1, cohesion_type='g') # each elem has enough cohesion
    neighb2_and_cohesion = clq.get_neighbours(k=1)
    if neighb2_and_cohesion is None: neighb2_and_cohesion= []
   # n = sorted(neighb_and_cohesion, key=operator.itemgetter('cohesion'))
    n2 = sorted(neighb2_and_cohesion, key=operator.itemgetter('cohesion'))

    #print [(e['cohesion'],e['clique'].get_id() ) for e in n]
    print [(e['cohesion'], e['clique'].get_id() ) for e in n2]

    neighbours = [(n['clique'], lev+1) for n in neighb2_and_cohesion]
    # H = nx.DiGraph()
    print 'level', lev


    vertices = []
    edg = []
    if lev < max_depth:
       for n in neighbours:
           H = launcher(n)
           e = e.union(H.edges)
           v = v.union(H.vertices)

    spark = SparkSession.builder.master("local[4]") \
        .appName("graph_clique") \
        .getOrCreate()
    # v = spark.createDataFrame([], ['id', "cohesion"])
    # e = spark.createDataFrame([], [ 'source', "target","weight"] )

    edges = spark.createDataFrame([(clq.get_id(), n['clique'].get_id(), clq.get_profile_vector_similarity_with(n['clique'])) for n in
             neighb2_and_cohesion],['source', 'target', 'weight'] )

    e = e.union(edges)
    e.show()

    spark.stop()



def launcher(t):
# type: (tuple)->nx.DiGraph
    return spark_neighbour_graph(*t)