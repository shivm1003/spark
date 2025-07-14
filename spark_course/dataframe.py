import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

from lib import utils

job_run_env = sys.argv[1].upper()
load_date = sys.argv[2]

print("Creating Spark Session")
spark = utils.get_spark_session(job_run_env)
print("Spark Session created")

if __name__ == '__main__':
    df = spark.read.format("csv").load("data/CompleteDataset.csv", inferSchema=True, header=True)

    # get the number of partitions
    # print(df.rdd.getNumPartitions())

    # repartition to 4
    df2 = df.repartition(4)
    # print(df2.rdd.getNumPartitions())

    ## Transformation ----
    # filter the columns
    df3 = df.where(df['Overall'] > 70)

    # Here we are using a Dataframe with SQL queries
    # To run the SQL queries, we need to register the DF to a temporary view

    df.createOrReplaceTempView("df_football")

    sql_query = """ select * from df_football where overall > 70
    """
    result = spark.sql(sql_query)
    ## user-defined function with dataframe

    # First, register the UDF
    spark.udf.register("UPPER", utils.uppercase_converter, StringType())
    # df.createOrReplaceTempView("df_football")
    sql_query = "SELECT Age, UPPER(Name) AS Name, UPPER(Club) AS Club FROM df_football"
    result = spark.sql(sql_query)
    result.show()