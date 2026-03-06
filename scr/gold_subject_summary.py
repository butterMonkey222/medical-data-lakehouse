from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count as Fcount, sum as Fsum, max as Fmax,
    when, collect_set, expr, concat
)
from delta import configure_spark_with_delta_pip



PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
participants_path = DATA_DIR / "Silver" / "participants_delta"
bids_files_path = DATA_DIR / "Silver" / "bids_files_delta"
phenotype_path = DATA_DIR / "Silver" / "phenotype_delta"
gold_path = DATA_DIR / "Gold" / "gold_subject_summary_delta"




builder = (
    SparkSession.builder
    .appName("gold-subject-summary")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

participants = spark.read.format("delta").load(str(participants_path))
bids_files   = spark.read.format("delta").load(str(bids_files_path))
phenotype    = spark.read.format("delta").load(str(phenotype_path))

# Rename join keys to "subject"
if "participant_id" in participants.columns and "subject" not in participants.columns:
    participants = participants.withColumnRenamed("participant_id", "subject")

if "participant_id" in phenotype.columns and "subject" not in phenotype.columns:
    phenotype = phenotype.withColumnRenamed("participant_id", "subject")

# Normalize subject format to "sub-xxx" everywhere
def normalize_subject(df):
    return df.withColumn(
        "subject",
        when(col("subject").startswith("sub-"), col("subject").cast("string"))
        .otherwise(concat(lit("sub-"), col("subject").cast("string")))
    )

participants = normalize_subject(participants)
phenotype = normalize_subject(phenotype)
bids_files = normalize_subject(bids_files)

# Coverage from bids_files
coverage = (
    bids_files
    .groupBy("subject")
    .agg(
        Fcount(lit(1)).alias("n_files_total"),
        Fsum(col("size_bytes").cast("long")).alias("total_size_bytes"),
        Fmax(when(col("datatype") == "anat", lit(1)).otherwise(lit(0))).alias("has_anat"),
        Fmax(when(col("datatype") == "func", lit(1)).otherwise(lit(0))).alias("has_func"),
        Fmax(when(col("datatype") == "dwi",  lit(1)).otherwise(lit(0))).alias("has_dwi"),
        Fmax(when(col("datatype") == "beh",  lit(1)).otherwise(lit(0))).alias("has_beh"),
        collect_set(col("task")).alias("tasks_raw")
    )
    .withColumn("tasks", expr("filter(tasks_raw, x -> x is not null)"))
    .drop("tasks_raw")
)

# Phenotype coverage
pheno_cov = (
    phenotype
    .groupBy("subject")
    .agg(
        Fcount(lit(1)).alias("n_phenotype_records"),
        lit(1).alias("has_phenotype")
    )
)

# Build Gold
gold = (
    participants.select("subject").dropna().distinct()
    .join(coverage, on="subject", how="left")
    .join(pheno_cov, on="subject", how="left")
    .fillna({
        "n_files_total": 0,
        "total_size_bytes": 0,
        "has_anat": 0,
        "has_func": 0,
        "has_dwi": 0,
        "has_beh": 0,
        "has_phenotype": 0,
        "n_phenotype_records": 0
    })
)

gold.write.format("delta").mode("overwrite").save(str(gold_path))

print("Wrote:", str(gold_path))
print("Rows:", gold.count())
gold.orderBy("subject").show(20, truncate=False)