from lib import utils
from pyspark.sql.functions import *

job_run_env = sys.argv[1].upper()
load_date = sys.argv[2]

print("Creating Spark Session")
spark = utils.get_spark_session(job_run_env)
print("Spark Session created")

if __name__ == '__main__':
    df = spark.read.text('data/kddcup.data.gz')
    split_col = split(df['value'], ',')
    df = (df.withColumn('protocol', split_col.getItem(1))
            .withColumn('service', split_col.getItem(2))
            .withColumn("flag", split_col.getItem(3))
            .withColumn("src_bytes", split_col.getItem(4))
            .withColumn("dst_bytes", split_col.getItem(5))
            .withColumn("urgent", split_col.getItem(8))
            .withColumn("num_failed_logins", split_col.getItem(10))
            .withColumn("root_shell", split_col.getItem(13))
            .withColumn("guest_login", split_col.getItem(21))
            .withColumn("label", split_col.getItem(41))
            .drop('value'))

    # 1. Count the number of connections for each label
    df.groupBy('label').count().orderBy('count', ascending=False)

    # 2. Get the list of protocols that are normal and vulnerable to attacks,
    # where there is not guest login to the destination address.
    df.createOrReplaceTempView('df_sql')
    query_1 = """select protocol,
                case 
                    when label like 'normal%' then 'no attack'
                    else 'attack'
                end as state,
                count(*) as freq
                from df_sql
                group by protocol, state
                order by protocol desc
            """
    spark.sql(query_1)

    # 3: Filter the number of icmp-based attacks for each service.
    query_2 = """select service, count(*) 
                from df_sql where protocol = 'icmp'
                group by service
                order by count(*) desc
            """
    spark.sql(query_2).show()


