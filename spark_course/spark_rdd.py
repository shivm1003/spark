import re

from pyspark import SparkConf, SparkContext, StorageLevel

conf = SparkConf().setAppName("spark rdd").setMaster("local[*]")
sc = SparkContext(conf=conf)

rdd = sc.textFile("/Users/shivampandey/Documents/work/personal/spark/spark/spark_course/data/ghtorrent-logs.txt.gz")
rdd.persist(StorageLevel.MEMORY_AND_DISK)


# 1. count the number of records and take 20 records only.
rdd_1 = rdd.takeSample(False, 20, 1234)

# 2. get the number of lines with both Transaction or Repo information.
def split_word(line):
    return re.compile(r'\w+').findall(line.lower())

rdd_transaction = rdd.filter(lambda x : 'transaction' in split_word(x))
rdd_repo = rdd.filter(lambda x : 'repo' in split_word(x))

rdd_intersect = rdd_transaction.intersection(rdd_repo)
# print(rdd_intersect.collect())

# 3. what are the most active downloader id for failed connections?
rdd_failed = rdd.filter(lambda x: 'failed' in split_word(x))
# print(rdd_failed.take(10))

# 4. what is the most active repository?
rdd_active = rdd.filter(lambda x : ' repo ' in split_word(x)) \
    .map(lambda x : x.lower().split('repo')[1].split(' ')[1]) \
    .map(lambda repo : (repo, 1)) \
    .reduce(lambda x,y : x + y) \
    .sortBy(lambda x : x[1], ascending=False) \

print(rdd_active.take(3))

