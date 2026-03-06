import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip



PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
ROOT = DATA_DIR / "Bronze" / "phenotype"
DELTA_PATH = DATA_DIR / "Silver" / "phenotype_delta"





# # Check if all tsv files have the participant_id column
# count = 0

# for p in ROOT.iterdir():
#     if p.is_file() and p.name.endswith(".tsv"):
#         df = pd.read_csv(p.resolve(), sep = "\t")
#         if "participant_id" in df.columns:
#             count = count + 1

# print(count)






tables = []

for p in ROOT.glob("*.tsv"):
    df_wide = pd.read_csv(p.resolve(), sep = "\t")
    df_long = df_wide.melt(
        id_vars = "participant_id",
        var_name = "phenotype",
        value_name = "value"
    )
    df_long["source_file"] = p.name
    df_long = df_long.dropna(subset=["value"])
        
    tables.append(df_long)

df = pd.concat(tables, ignore_index=True)   

df["value"] = df["value"].astype(str)
df["participant_id"] = df["participant_id"].astype(str)  




builder = (
    SparkSession.builder
    .appName("bids-files-to-delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()


sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").save(str(DELTA_PATH))

