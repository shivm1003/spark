from lib.dataloader import read_address_file, read_party_file, read_account_file
import uuid

from pyspark.sql.functions import struct, lit, col, to_json, array


def data_join():
    account_df = read_account_file()
    # account_party_address = party_address_df.join(account_df, party_address_df.account_id == account_df.account_id)

    return account_df

def insert_operation(column, alias):
    return struct(
        lit("INSERT").alias("operation"),
        column.alias("newValue"),
        lit(None).alias("oldValue"),
    ).alias(alias)


def contract_identifier(df):
    df_with_audit = df.select("account_id",
                              insert_operation(col("account_id"), "contractIdentifier"),
                              insert_operation(col("source_sys"), "sourceSystemIdentifier"),
                              insert_operation(col("account_start_date"), "contactStartDateTime"),
                              insert_operation(col("branch_code"), "contractBranchCode"),
                              insert_operation(col("country"), "contractCountry"),
                              )
    df_json = df_with_audit.select("account_id",
                                   to_json(struct(col("contractIdentifier"),
                                                  col("sourceSystemIdentifier"),
                                                  col("contactStartDateTime"),
                                                  col("contractBranchCode"),
                                                  col("contractCountry"))).alias("contract_payload")
                                   )
    return df_json

def contract_title(df):
    contract_df = array(
        struct(
        lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
        col("legal_title_1").alias("contractTitle")),
        struct(
            lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
            col("legal_title_2").alias("contractTitle")))

    # filter condition is left, will take it later..

    df_results = df.select("account_id", insert_operation(contract_df, "contractTitle"))
    df_json = df_results.select("account_id", to_json(struct(col("contractTitle"))).alias("title_value"))
    return df_json


def tax_identifier(df):
    tax_struct =  struct(col("tax_id_type").alias("taxIdType"), col("tax_id").alias("taxIdentifier"))
    df_struct = df.select("account_id", insert_operation(tax_struct, "taxIdentifier"))
    tax_json = df_struct.select("account_id", to_json(struct(col("taxIdentifier"))).alias("tax_value"))
    return tax_json

def part_join():
    party_df = read_party_file()
    party_df = party_df.select("party_id", "relation_type",
                               "relation_start_date", col("account_id").alias("party_account_id"))
    address_df = read_address_file()
    address_df = address_df.select("address_line_1","address_line_2",
                                   "city","postal_code", "country_of_address",
                                   "address_start_date",
                                   col("party_id").alias("party_address_id")
                                   )
    # join all the tables
    party_address_df = party_df.join(address_df,
                                     party_df.party_id == address_df.party_address_id,
                                     how="left")
    return party_address_df

def party_relations_address(df):
    struct_address = struct(col("address_line_1").alias("addressLine1"),
           col("address_line_2").alias("addressLine2"),
           col("city").alias("addressCity"),
           col("postal_code").alias("addressPostalCode"),
           col("country_of_address").alias("addressCountry"),
           col("address_start_date").alias("addressStartDate"))

    return df.select("party_account_id", "party_id",
                     insert_operation(col("party_id"), "partyIdentifier"),
                     insert_operation(col("relation_type"), "partyRelationshipType"),
                     insert_operation(col("relation_start_date"), "partyRelationStartDateTime"),
                     insert_operation(struct_address, "partyAddress"),)



def generate_unique_id():
    """
    Generates a unique UUID (version 4) each time it's called.
    Returns:
        str: A unique UUID string in standard 8-4-4-4-12 format.
    """
    return str(uuid.uuid4())
