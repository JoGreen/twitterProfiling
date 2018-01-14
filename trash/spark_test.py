import findspark
findspark.init()

#os.environ["SPARK_HOME"] = "/usr/local/Cellar/apache-spark/2.2.0"
#os.environ["JAVA_HOME"] = "/usr/libexec/java_home"

from operator import add

from pyspark import SparkContext
from pyspark.sql import SparkSession

# if __name__ == "__main__":
# sc = SparkContext(appName="PythonWordCount")
# lines = sc.textFile("sample.txt", 1)
# counts = lines.flatMap(lambda x: x.split(' ')) \
#     .map(lambda x: (x, 1)) \
#     .reduceByKey(add)
# output = counts.collect()
# for (word, count) in output:
#     try:
#         print("%s: %i" % (word, count))
#     except Exception: print "encode error"
#
# sc.stop()

spark = SparkSession.builder.master("local[4]")\
    .appName("Word Count")\
    .getOrCreate()
# Create a Vertex DataFrame with unique ID column "id"
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
], ["id", "name", "age"])
# Create an Edge DataFrame with "src" and "dst" columns
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
], ["src", "dst", "relationship"])
# Create a GraphFrame
from graphframes import *
g = GraphFrame(v, e)

# Query: Get in-degree of each vertex.
g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.3, maxIter=1)
#results.vertices.select("id", "pagerank").show()
display(results.vertices)

spark.stop()
