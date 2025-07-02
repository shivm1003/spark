from pyspark.sql.types import *
from lib. Utils import get_spark_session

def account_schema():
    define_schema = StructType([
            StructField("load_date", DateType(), True),
            StructField("active_ind", StringType(), True),
            StructField("account_id", LongType(), True),
            StructField("source_sys", StringType(), True),
            StructField("account_start_date", TimestampType(), True),
            StructField("legal_title_1", StringType(), True),
            StructField("legal_title_2", StringType(), True),
            StructField("tax_id_type", StringType(), True),
            StructField("tax_id", StringType(), True),
            StructField("branch_code", StringType(), True),
            StructField("country", StringType(), True),
    ])
    return define_schema

def party_schema():
    define_schema = StructType([
        StructField("load_date", DateType(), True),
        StructField("account_id", LongType(), True),
        StructField("party_id", LongType(), True),
        StructField("relation_type", StringType(), True),
        StructField("relation_start_date", TimestampType(), True),
    ])

def address_schema():
    define_schema = StructType([
        StructField("load_date", DateType(), True),
        StructField("party_id", LongType(), True),
        StructField("address_line_1", StringType(), True),
        StructField("address_line_2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country_of_address", StringType(), True),
        StructField("address_start_date", TimestampType(), True)
    ])
    return define_schema

def read_account_file():
    spark = get_spark_session("LOCAL")
    account_df = spark.read.csv("./test_data/accounts/account_samples.csv",
                        header=True, schema=account_schema())
    return account_df

def read_party_file():
    spark = get_spark_session("LOCAL")
    party_df = spark.read.csv("./test_data/parties/party_samples.csv",
                        header=True, schema=party_schema())
    return party_df

def read_address_file():
    spark = get_spark_session("LOCAL")
    address_df = spark.read.csv("./test_data/party_address/address_samples.csv",
                                header=True, schema=address_schema())
    return address_df