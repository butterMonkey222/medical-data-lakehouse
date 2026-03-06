import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip




PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
ROOT = DATA_DIR / "Bronze"
DELTA_PATH = DATA_DIR / "Silver" / "bids_files_delta"





def get_extension(filename):

    if filename.endswith(".nii.gz"):
        return ".nii.gz"

    if filename.endswith(".tsv.gz"):
        return ".tsv.gz"

    return "." + filename.split(".")[-1]

def get_task(filename):

    parts = filename.split("_")

    for p in parts:
        if p.startswith("task-"):
            return p.replace("task-", "")

    return None

def get_suffix(filename):

    name = filename

    if name.endswith(".nii.gz"):
        name = name[:-7]

    elif name.endswith(".tsv.gz"):
        name = name[:-7]

    else:
        name = name.rsplit(".", 1)[0]

    return name.split("_")[-1]

def parse_bids_name(filename):

    ext = get_extension(filename)
    task = get_task(filename)
    suffix = get_suffix(filename)

    return task, suffix, ext









rows = []

for p in ROOT.iterdir():
    if not (p.name.startswith("sub-") and p.is_dir()):
        continue

    subject = p.name

    for a in p.iterdir():

        if not a.is_dir():
            continue

        datatype = a.name

        for t in a.iterdir():

            if not t.is_file():
                continue

            task, suffix, ext = parse_bids_name(t.name)
            relpath = str(t.relative_to(ROOT))
            size_bytes = t.stat().st_size
            is_sidecar = ext == ".json"
            filename = t.name

            rows.append({"subject": subject,
                         "datatype": datatype,
                         "suffix": suffix,
                         "task": task,
                         "extension": ext,
                         "filename": filename,
                         "relpath": relpath,
                         "size_bytes": size_bytes,
                         "is_sidecar": is_sidecar})
                

df = pd.DataFrame(rows)








builder = (
    SparkSession.builder
    .appName("bids-files-to-delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()


sdf = spark.createDataFrame(df)
sdf.write.format("delta").mode("overwrite").save(str(DELTA_PATH))