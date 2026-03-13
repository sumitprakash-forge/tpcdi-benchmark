"""
CustomerMgmt XML schema definition as StructType.

This schema matches customer_mgmt_schema.json and can be used directly
with spark-xml reader instead of loading from JSON.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType, TimestampType
)


def get_customer_mgmt_schema() -> StructType:
    """
    Returns the CustomerMgmt XML schema as a StructType.
    
    This matches the structure in customer_mgmt_schema.json:
    - Top level: Customer (struct), _ActionTS (timestamp), _ActionType (string)
    - Customer contains nested structs: Account, Address, ContactInfo, Name, TaxInfo
    - Plus direct fields: _C_DOB, _C_GNDR, _C_ID, _C_TAX_ID, _C_TIER
    """
    return StructType([
        StructField("Customer", StructType([
            StructField("Account", StructType([
                StructField("CA_B_ID", LongType(), True),
                StructField("CA_NAME", StringType(), True),
                StructField("_CA_ID", LongType(), True),
                StructField("_CA_TAX_ST", LongType(), True),
            ]), True),
            StructField("Address", StructType([
                StructField("C_ADLINE1", StringType(), True),
                StructField("C_ADLINE2", StringType(), True),
                StructField("C_CITY", StringType(), True),
                StructField("C_CTRY", StringType(), True),
                StructField("C_STATE_PROV", StringType(), True),
                StructField("C_ZIPCODE", StringType(), True),
            ]), True),
            StructField("ContactInfo", StructType([
                StructField("C_ALT_EMAIL", StringType(), True),
                StructField("C_PHONE_1", StructType([
                    StructField("C_AREA_CODE", LongType(), True),
                    StructField("C_CTRY_CODE", LongType(), True),
                    StructField("C_EXT", LongType(), True),
                    StructField("C_LOCAL", StringType(), True),
                ]), True),
                StructField("C_PHONE_2", StructType([
                    StructField("C_AREA_CODE", LongType(), True),
                    StructField("C_CTRY_CODE", LongType(), True),
                    StructField("C_EXT", LongType(), True),
                    StructField("C_LOCAL", StringType(), True),
                ]), True),
                StructField("C_PHONE_3", StructType([
                    StructField("C_AREA_CODE", LongType(), True),
                    StructField("C_CTRY_CODE", LongType(), True),
                    StructField("C_EXT", LongType(), True),
                    StructField("C_LOCAL", StringType(), True),
                ]), True),
                StructField("C_PRIM_EMAIL", StringType(), True),
            ]), True),
            StructField("Name", StructType([
                StructField("C_F_NAME", StringType(), True),
                StructField("C_L_NAME", StringType(), True),
                StructField("C_M_NAME", StringType(), True),
            ]), True),
            StructField("TaxInfo", StructType([
                StructField("C_LCL_TX_ID", StringType(), True),
                StructField("C_NAT_TX_ID", StringType(), True),
            ]), True),
            StructField("_C_DOB", DateType(), True),
            StructField("_C_GNDR", StringType(), True),
            StructField("_C_ID", LongType(), True),
            StructField("_C_TAX_ID", StringType(), True),
            StructField("_C_TIER", StringType(), True),
        ]), True),
        StructField("_ActionTS", TimestampType(), True),
        StructField("_ActionType", StringType(), True),
    ])


# Example usage:
# from benchmark.etl.bronze.customer_mgmt_schema_definition import get_customer_mgmt_schema
# schema = get_customer_mgmt_schema()
# df = spark.read.format("xml").option("rowTag", "TPCDI:Action").schema(schema).load("path/to/CustomerMgmt.xml")
