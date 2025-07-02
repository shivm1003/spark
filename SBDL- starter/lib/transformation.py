from lib.dataloader import read_address_file, read_account_file, read_party_file
import uuid

from pyspark.sql.functions import struct, lit, col, to_json, array, collect_list


def insert_operation(column, alias):
    return struct(
        lit("INSERT").alias("operation"),
        column.alias("newValue"),
    ).alias(alias)

def keys_struct(df):
    struct_df =  df.select("account_id", struct(lit("contractIdentifier").alias("keyField"),
                     col("account_id").alias("keyValue")).alias("keys"))

    df_json = struct_df.select("account_id", to_json(col("keys")).alias("keys"))

    return df_json


def account_details(df):
    contract_struct = array(
        struct(
        lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
        col("legal_title_1").alias("contractTitle")),
        struct(
            lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
            col("legal_title_2").alias("contractTitle")))

    # filter condition is left, will take it later..
    tax_struct = struct(col("tax_id_type").alias("taxIdType"), col("tax_id").alias("taxIdentifier"))

    df_contract_title = df.select("account_id",
                                  insert_operation(col("account_id"), "contractIdentifier"),
                                  insert_operation(col("source_sys"), "sourceSystemIdentifier"),
                                  insert_operation(col("account_start_date"), "contactStartDateTime"),
                                  insert_operation(contract_struct, "contractTitle"),
                                  insert_operation(tax_struct, "taxIdentifier"),
                                  insert_operation(col("branch_code"), "contractBranchCode"),
                                  insert_operation(col("country"), "contractCountry"),
                                  )
    return df_contract_title

def party_join():
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

def party_relations_address():
    struct_address = struct(col("address_line_1").alias("addressLine1"),
           col("address_line_2").alias("addressLine2"),
           col("city").alias("addressCity"),
           col("postal_code").alias("addressPostalCode"),
           col("country_of_address").alias("addressCountry"),
           col("address_start_date").alias("addressStartDate"))

    df = party_join()

    df_prd = df.select("party_account_id", "party_id",
                     insert_operation(col("party_id"), "partyIdentifier"),
                     insert_operation(col("relation_type"), "partyRelationshipType"),
                     insert_operation(col("relation_start_date"), "partyRelationStartDateTime"),
                     insert_operation(struct_address, "partyAddress"),)
    return df_prd


def join_account_party(df):
    party_df = party_relations_address()
    join_df = df.join(party_df,df.account_id == party_df.party_account_id, how="left_outer")

    return join_df

def group_data(df):
    # read account data
    party_relation_struct = struct(col("partyIdentifier").alias("partyIdentifier"),
                                   col("partyRelationshipType").alias("partyRelationshipType"),
                                   col("partyRelationStartDateTime").alias("partyRelationStartDateTime"),
                                   col("partyAddress").alias("partyAddress"),).alias("partyRelation")

    df_grouped = (df.select("account_id", party_relation_struct).
                  groupBy("account_id").agg(collect_list("partyRelation").alias("partyRelations")))
    return df_grouped

def generate_unique_id():
    """
    Generates a unique UUID (version 4) each time it's called.
    Returns:
        str: A unique UUID string in standard 8-4-4-4-12 format.
    """
    return str(uuid.uuid4())
