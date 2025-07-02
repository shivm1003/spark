import sys
from lib import Utils
from lib. logger import Log4j
# from lib. dataloader import read_account_file, read_party_file, read_address_file
from lib.transformation import (data_join, contract_identifier, contract_title, tax_identifier,
                                part_join, party_relations_address)

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
    # data_df = data_join()

    # data_df.show()
    # data_df.printSchema()
    party_df = part_join()
    df = party_relations_address(party_df)

    df.show()
    df.printSchema()

    # new_df = contract_identifier(data_df)
    # new_df.show(truncate=False)


    logger.info("Finished creating Spark Session")
