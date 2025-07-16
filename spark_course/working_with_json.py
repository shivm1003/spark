import re
import sys

from pyspark.sql.types import StructField

from lib import utils
from pyspark.sql.functions import *

job_run_env = sys.argv[1].upper()
load_date = sys.argv[2]

print("Creating Spark Session")
spark = utils.get_spark_session(job_run_env)
print("Spark Session created")

if __name__ == '__main__':
    # define schema to read the JSON file
    schema = StructType([
        StructField("authors", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("license", StringType(), True),
        StructField("comments", StringType(), True),
        StructField("abstract", StringType(), True),
        StructField("versions", ArrayType(StringType()), True),
    ])
    df = spark.read.json("data/arxiv-metadata-oai-snapshot.json", schema=schema)


    #1. Delete the missing values
    # drop
    df = df.dropna(subset=["comments"])

    # Fill the null values with unknown
    df = df.fillna(value="unknown", subset=["license"])

    # 2. Get the author's name who published the paper in the math category
    # We will be using SQL or UDF, because they have the optimisation techniques, where
    # Dataframe does not have its own optimisation, it uses a JVM machine

    # create a temp view to register the dataframe as SQL tables
    df.createOrReplaceTempView("df_archieve")

    query_1 = "select * from df_archieve where categories like '%math%'"
    sql_df = spark.sql(query_1)

    # 3. Get a license with more than 5-letter words in the abstract
    query_2 = """ select distinct(license)
               from df_archieve
               where abstract REGEXP '%(([A-Za-z][^_/\\<>]{5,})\)'"""
    sql_df_2 = spark.sql(query_2)

    # 4. Get the average number of pages
    def get_pages(colname):
        search = re.findall(r'\d+ pages', colname)
        if search:
            return int(search[0].split(" ")[0])
        else:
            return 0

    # register the udf
    spark.udf.register("get_pages", get_pages)

    query_3 = """select AVG(get_pages(comments)) as AVG, 
                SUM(get_pages(comments)) as SUM 
                from df_archieve
                where license = 'unknown'
                """

    # spark.sql(query_3).show()
    df.show()



