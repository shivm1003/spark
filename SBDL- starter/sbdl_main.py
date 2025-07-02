import sys
from lib import Utils
from lib. logger import Log4j
from lib. dataloader import read_account_file
from lib.transformation import account_details, join_account_party, group_data, keys_struct

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    # Spark session
    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    # Read the account file
    account_df = read_account_file()
    struct_account = account_details(account_df)

    # account and party dataframe join
    join_df = join_account_party(account_df)

    # group of the data
    group_df = group_data(join_df)
    group_df = group_df.withColumnRenamed("account_id", "group_account_id")

    final_df = struct_account.join(group_df, struct_account.account_id == group_df.group_account_id, how='inner')

    df_keys = keys_struct(account_df)
    final_df.show(truncate=False)

    logger.info("Finished creating Spark Session")
