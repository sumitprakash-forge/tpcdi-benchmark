"""
Parallel parsing of CustomerMgmt.xml using a Python UDTF on Databricks/Spark 3.5+.

Splits the XML file into chunks (by Action elements), then uses a UDTF with
LATERAL VIEW to parse each chunk in parallel. Creates or replaces a Unity Catalog
(UC) Python UDTF in the passed catalog and schema via CREATE OR REPLACE FUNCTION,
then uses that function in the lateral view. Falls back to spark-xml read if this path fails.
"""

import logging
import re
from typing import Iterator, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

# Delimiter used when joining Action XML strings into a chunk (must not appear inside XML)
ACTION_BOUNDARY = "\n<!--ACTION_BOUNDARY-->\n"


def _split_actions(content: str) -> list:
    """Extract individual Action elements from XML content. Handles TPCDI:Action or Action."""
    # Match <Action ...>...</Action> or <TPCDI:Action ...>...</TPCDI:Action>
    pattern = re.compile(
        r"(<(?:(?:TPCDI:)?Action)[^>]*>.*?</(?:(?:TPCDI:)?Action)>)",
        re.DOTALL,
    )
    return pattern.findall(content)


def _chunk_list(lst: list, k: int) -> list:
    """Partition list into k roughly equal chunks."""
    if k <= 0 or not lst:
        return [lst] if lst else []
    n = len(lst)
    k = min(k, n)
    size, remainder = divmod(n, k)
    chunks = []
    start = 0
    for i in range(k):
        end = start + size + (1 if i < remainder else 0)
        chunks.append(lst[start:end])
        start = end
    return chunks


def read_customer_mgmt_with_udtf(
    spark,
    full_path: str,
    num_chunks: int = 64,
    row_tag: str = "TPCDI:Action",
    root_tag: Optional[str] = "TPCDI:Actions",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> Optional["DataFrame"]:
    """
    Read CustomerMgmt.xml using a UDTF (LATERAL VIEW) to parse chunks in parallel.
    Registers the UDTF in the given catalog and schema so SQL can resolve it.
    Requires Spark 3.5+ (e.g. Databricks DBR 14.3+). Returns a DataFrame
    with the same schema as spark-xml read (nested struct per Action).
    """
    from pyspark.sql.functions import col, udtf

    # 1) Get schema from a tiny spark-xml read (same as current loader)
    try:
        schema_df = (
            spark.read.format("xml")
            .option("rowTag", row_tag)
            .option("rootTag", root_tag or "root")
            .load(full_path)
            .limit(1)
        )
        action_schema = schema_df.schema
    except Exception as e:
        logger.warning(f"Could not get schema from spark-xml for UDTF path: {e}")
        return None

    # 2) Read file as binary, split into chunks on the executor (avoid driver OOM)
    binary_df = spark.read.format("binaryFile").load(full_path)
    num_partitions = max(1, min(num_chunks, 256))

    def _partition_to_chunks(rows):
        for row in rows:
            content = row.content
            if not content:
                continue
            content_str = content.decode("utf-8", errors="replace")
            actions = _split_actions(content_str)
            if not actions:
                continue
            chunks = _chunk_list(actions, num_partitions)
            for i, chunk_actions in enumerate(chunks):
                yield (i, ACTION_BOUNDARY.join(chunk_actions))

    from pyspark.sql.types import IntegerType, StringType
    from pyspark.sql import Row
    chunk_rdd = binary_df.rdd.mapPartitions(_partition_to_chunks)
    chunks_df = spark.createDataFrame(
        chunk_rdd.map(lambda x: Row(chunk_id=x[0], chunk_content=x[1])),
        "chunk_id: int, chunk_content: string",
    ).repartition(num_partitions)
    chunks_df.createOrReplaceTempView("_customer_mgmt_chunks")

    # 4) Create or replace Unity Catalog (UC) Python UDTF in the passed catalog and schema.
    # LATERAL VIEW must use unqualified name (qualified names unsupported); we set USE CATALOG/SCHEMA before calling.
    # Save and restore session catalog/schema so we don't affect downstream ETL (e.g. silver_industry).
    will_change_context = bool(catalog and schema) or bool(schema)
    saved_catalog = spark.sql("SELECT current_catalog()").first()[0] if will_change_context else None
    saved_schema = spark.sql("SELECT current_schema()").first()[0] if will_change_context else None

    udtf_name = "parse_customer_mgmt_chunk"
    if not catalog or not schema:
        # Fallback: session-scoped registration when catalog/schema not provided
        @udtf(returnType="action_ordinal: int, action_xml: string")
        class ParseCustomerMgmtChunk:
            def eval(self, chunk_id: int, chunk_content: str) -> Iterator[tuple]:
                if not chunk_content:
                    return
                parts = chunk_content.split(ACTION_BOUNDARY)
                for i, action_xml in enumerate(parts):
                    action_xml = action_xml.strip()
                    if action_xml:
                        yield (i, action_xml)

        if schema:
            spark.sql(f"USE SCHEMA `{schema}`")
        spark.udtf.register(udtf_name, ParseCustomerMgmtChunk)
    else:
        # UC Python UDTF: CREATE OR REPLACE FUNCTION catalog.schema.parse_customer_mgmt_chunk
        # Python body must match ACTION_BOUNDARY and eval logic (no $$ inside the AS $$ block).
        _PY_UDTF_BODY = r'''
class ParseCustomerMgmtChunk:
    def eval(self, chunk_id: int, chunk_content: str):
        if not chunk_content:
            return
        boundary = "\n<!--ACTION_BOUNDARY-->\n"
        parts = chunk_content.split(boundary)
        for i, action_xml in enumerate(parts):
            ax = action_xml.strip()
            if ax:
                yield (i, ax)
'''
        create_uc_sql = (
            f"CREATE OR REPLACE FUNCTION `{catalog}`.`{schema}`.`{udtf_name}`("
            "chunk_id INT, chunk_content STRING)\n"
            "RETURNS TABLE(action_ordinal INT, action_xml STRING)\n"
            "LANGUAGE PYTHON\n"
            "HANDLER 'ParseCustomerMgmtChunk'\n"
            "AS $$" + _PY_UDTF_BODY + "$$;"
        )
        try:
            spark.sql(create_uc_sql)
            logger.info(f"Created/replaced UC Python UDTF: {catalog}.{schema}.{udtf_name}")
        except Exception as e:
            logger.warning(
                f"Could not create UC UDTF (requires DBR 17.1+ or serverless), "
                f"falling back to session-scoped registration: {e}"
            )
            @udtf(returnType="action_ordinal: int, action_xml: string")
            class ParseCustomerMgmtChunk:
                def eval(self, chunk_id: int, chunk_content: str) -> Iterator[tuple]:
                    if not chunk_content:
                        return
                    parts = chunk_content.split(ACTION_BOUNDARY)
                    for i, action_xml in enumerate(parts):
                        action_xml = action_xml.strip()
                        if action_xml:
                            yield (i, action_xml)
            spark.sql(f"USE CATALOG `{catalog}`")
            spark.sql(f"USE SCHEMA `{schema}`")
            spark.udtf.register(udtf_name, ParseCustomerMgmtChunk)
        else:
            spark.sql(f"USE CATALOG `{catalog}`")
            spark.sql(f"USE SCHEMA `{schema}`")

    # 5) LATERAL join: each chunk row produces multiple (action_ordinal, action_xml) rows.
    # Use LATERAL tvf(args) AS alias (modern syntax); LATERAL VIEW can cause NOT_A_SCALAR_FUNCTION for UC UDTFs.
    lateral_sql = f"""
        SELECT c.chunk_id, p.action_ordinal, p.action_xml
        FROM _customer_mgmt_chunks c, LATERAL {udtf_name}(c.chunk_id, c.chunk_content) AS p
    """
    parsed_rows = spark.sql(lateral_sql)

    # Restore session catalog/schema so downstream ETL (silver, etc.) is unaffected
    if saved_catalog is not None and saved_schema is not None:
        try:
            spark.sql(f"USE CATALOG `{saved_catalog}`")
            spark.sql(f"USE SCHEMA `{saved_schema}`")
        except Exception as e:
            logger.warning(f"Could not restore catalog/schema: {e}")

    # 6) Parse action_xml into struct using from_xml (Spark 4.0+ / DBR 14.3+ with spark-xml)
    try:
        from pyspark.sql import functions as F
        from_xml = getattr(F, "from_xml", None)
        if from_xml is not None:
            parsed_rows = parsed_rows.withColumn(
                "_parsed", from_xml(col("action_xml"), action_schema)
            )
            # Flatten to match spark-xml read: select struct columns as top-level
            struct_fields = parsed_rows.select("_parsed.*").schema.fields
            parsed_rows = parsed_rows.select(
                *[col("_parsed." + f.name).alias(f.name) for f in struct_fields],
            )
            return parsed_rows
    except Exception as e:
        logger.warning(f"from_xml not available for UDTF path: {e}")

    # Fallback: from_xml not available (e.g. Spark 3.5 without backport)
    return None
