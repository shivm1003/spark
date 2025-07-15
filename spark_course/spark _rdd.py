from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark rdd").setMaster("local[*]")
sc = SparkContext(conf=conf)

rdd = sc.textFile("/Users/shivampandey/Documents/work/personal/spark/spark/spark_course/data/ghtorrent-logs.txt.gz")
print(rdd.count())