import configparser
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config('spark.sql.autoBroadcastJoinThreshold', -1) \
            .config('spark.sql.adaptive.enabled', 'false') \
            .master("local[*]") \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        pass


def uppercase_converter(columnar):
    if len(columnar) > 10:
        return columnar.upper()
    else:
        return columnar.lower()
