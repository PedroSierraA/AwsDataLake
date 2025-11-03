import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import to_utc_timestamp, input_file_name, regexp_extract
from awsglue.job import Job
from pyspark.sql.functions import year, month
import pyspark.sql.functions as F

## --- Inicialización del contexto Glue ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

## --- Lectura desde el Data Catalog (raw) ---
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="datalake-catalog-taxis",
    table_name="raw_raw"
)
df = dyf.toDF()

## --- 🔧 Transformaciones de limpieza ---
df = df.filter(F.col("VendorID").isin([1, 2, 6, 7]))
df = df.filter(F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
df = df.withColumn(
    "passenger_count",
    F.when((F.col("passenger_count") == 0) | (F.col("passenger_count").isNull()), 1)
    .otherwise(F.col("passenger_count"))
)
df = df.filter(F.col("passenger_count") <= 8)
df = df.filter((F.col("trip_distance") > 0) & (F.col("trip_distance") <= 50))
df = df.withColumn(
    "RatecodeID",
    F.when(F.col("RatecodeID").isin([1,2,3,4,5,6,99]), F.col("RatecodeID")).otherwise(1)
)
df = df.withColumn(
    "store_and_fwd_flag",
    F.when(F.upper(F.col("store_and_fwd_flag")) == "Y", "Y").otherwise("N")
)
df = df.filter(
    (F.col("PULocationID").between(1, 266)) & (F.col("DOLocationID").between(1, 266))
)
df = df.withColumn(
    "payment_type",
    F.when(F.col("payment_type").isin([0,1,2,3,4,5,6]), F.col("payment_type")).otherwise(1)
)
df = df.filter(F.col("fare_amount") > 0)
df = df.filter((F.col("total_amount") > 0) & (F.col("total_amount") >= F.col("fare_amount")))
df = df.fillna({
    "tip_amount": 0.0,
    "tolls_amount": 0.0,
    "congestion_surcharge": 0.0,
    "Airport_fee": 0.0,
    "cbd_congestion_fee": 0.0
})
df = df.filter((F.col("improvement_surcharge") >= 0) & (F.col("mta_tax") >= 0))
df = df.withColumn(
    "trip_duration_min",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
)
df = df.filter((F.col("trip_duration_min") > 0) & (F.col("trip_duration_min") <= 120))

# Redondear montos
monetary_cols = [
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge",
    "Airport_fee", "cbd_congestion_fee"
]
for c in monetary_cols:
    df = df.withColumn(c, F.round(F.col(c), 2))

df = df.withColumn("tpep_pickup_datetime", to_utc_timestamp("tpep_pickup_datetime", "UTC"))
df = df.withColumn("tpep_dropoff_datetime", to_utc_timestamp("tpep_dropoff_datetime", "UTC"))

# Recalcular particiones finales (por año/mes reales)
df = df.withColumn("year", year("tpep_pickup_datetime"))
df = df.withColumn("month", month("tpep_pickup_datetime"))
df = df.filter(F.col("year").isin([2024, 2025]))

## --- Escritura a la zona curated ---
dyf_clean = DynamicFrame.fromDF(df, glueContext, "dyf_clean")

sink = glueContext.getSink(
    path="s3://datalake-demo-pedro/cleaned/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year", "month"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="sink"
)
sink.setCatalogInfo(
    catalogDatabase="datalake-catalog-taxis",
    catalogTableName="taxi_cleaned"
)
sink.setFormat("glueparquet")
sink.writeFrame(dyf_clean)

job.commit()
