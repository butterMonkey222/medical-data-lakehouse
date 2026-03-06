from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
ROOT = DATA_DIR / "Bronze" / "participants.tsv"
DELTA_PATH = DATA_DIR / "Silver" / "participants_delta"



builder = (
    SparkSession.builder
    .appName("participants-to-delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


df = (spark.read
      .option("header", True)
      .option("sep", "\t")
      .option("inferSchema", True)
      .csv(str(ROOT)))


df.write.format("delta").mode("overwrite").save(str(DELTA_PATH))