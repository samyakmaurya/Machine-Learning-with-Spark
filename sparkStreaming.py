from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession


sc = SparkContext(appName="stream")
ssc = StreamingContext(sc, 5)
spark = SparkSession(sc)

def main(lines): 
    exists = len(lines.collect())
    if exists:
        df = lines.map(lambda x: x.split(",")).toDF()
        df.show()

lines = ssc.socketTextStream("localhost", 6100)
lines.foreachRDD(main)
		
ssc.start()
ssc.awaitTermination()
ssc.stop()


"""from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

def result(rdd):
    print(rdd)

sc = SparkContext("local[2]", "SentimentAnalysis")
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream("localhost", 6100)

words = lines.flatMap(lambda line: line.split(" "))
rdd = words.foreachRDD(result)
print(type(rdd))
print(rdd)"""

"""words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)"""

# Print the first ten elements of each RDD generated in this DStream to the console
##wordCounts.pprint()

"""# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

 # Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()"""


ssc.start()             # Start the computation
ssc.awaitTermination()