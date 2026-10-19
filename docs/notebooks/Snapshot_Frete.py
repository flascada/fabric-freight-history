"""
====================================================
Freight History Tracking
Microsoft Fabric + PySpark
====================================================

Purpose:
Create historical snapshots of freight data
only when real changes are detected.

Architecture:
Semantic Model
    ↓
Dataflow Gen2
    ↓
Lakehouse
    ↓
Current Freight Table
    ↓
PySpark Notebook
    ↓
Historical Snapshot

Features:
- Hash based change detection
- Snapshot versioning
- Duplicate prevention
- Lakehouse integration
"""

# ----------------------------------------
# READ CURRENT FREIGHT TABLE
# ----------------------------------------

df = spark.read.table("frete")

# ----------------------------------------
# STANDARDIZE COLUMN NAMES
# ----------------------------------------

snapshot = (
    df
    .withColumnRenamed(
        "Cidade Destino",
        "Cidade_Destino"
    )
    .withColumnRenamed(
        "Terminal Ajustado",
        "Terminal_Ajustado"
    )
)

# ----------------------------------------
# HASH GENERATION
# ----------------------------------------

from pyspark.sql import functions as F
import hashlib

# Exclude technical load date
columns_for_hash = [
    c for c in snapshot.columns
    if c != "Data_Carga"
]

json_data = (
    snapshot
    .select(*columns_for_hash)
    .orderBy(*columns_for_hash)
    .toPandas()
    .to_json()
)

current_hash = hashlib.md5(
    json_data.encode("utf-8")
).hexdigest()

print(f"Current hash: {current_hash}")

# ----------------------------------------
# CHECK IF CONTROL TABLE EXISTS
# ----------------------------------------

tables = [
    t.tableName
    for t in spark.sql(
        "SHOW TABLES"
    ).collect()
]

# ----------------------------------------
# FIRST EXECUTION
# ----------------------------------------

if "controle_frete" not in tables:

    print("First execution")

    spark.createDataFrame(
        [(current_hash,)],
        ["hash_dados"]
    ).write.mode(
        "overwrite"
    ).saveAsTable(
        "controle_frete"
    )

    snapshot.write.mode(
        "overwrite"
    ).saveAsTable(
        "frete_snapshot"
    )

# ----------------------------------------
# CHANGE DETECTION
# ----------------------------------------

else:

    control = spark.read.table(
        "controle_frete"
    )

    previous_hash = (
        control.collect()[0]["hash_dados"]
    )

    print(
        f"Previous hash: {previous_hash}"
    )

    print(
        f"Current hash: {current_hash}"
    )

    # ------------------------------------
    # NO CHANGES
    # ------------------------------------

    if current_hash == previous_hash:

        print(
            "No changes detected."
        )

    # ------------------------------------
    # CHANGES DETECTED
    # ------------------------------------

    else:

        print(
            "Changes detected."
        )

        snapshot.write.mode(
            "append"
        ).saveAsTable(
            "frete_snapshot"
        )

        spark.createDataFrame(
            [(current_hash,)],
            ["hash_dados"]
        ).write.mode(
            "overwrite"
        ).saveAsTable(
            "controle_frete"
        )

        print(
            "Snapshot created."
        )
