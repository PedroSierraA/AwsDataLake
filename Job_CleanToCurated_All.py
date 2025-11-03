import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import to_utc_timestamp, year, month, dayofmonth, avg, sum, round, col, count

## --- Inicialización del contexto Glue ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

## --- Lectura de toda la zona cleaned ---
dyf_clean = glueContext.create_dynamic_frame.from_catalog(
    database="datalake-catalog-taxis",
    table_name="cleaned"
)
df = dyf_clean.toDF()


## --- Consulta 1: Airport Fee ---
df_airport = df.filter(col("airport_fee") > 0)
df_airport_summary = df_airport.groupBy("year", "month").agg(
    avg("passenger_count").alias("avg_passenger_count"),
    sum("passenger_count").alias("total_passengers"),
    count("*").alias("num_trips")
)

dyf_airport = DynamicFrame.fromDF(df_airport_summary, glueContext, "dyf_airport")
sink_airport = glueContext.getSink(
    path="s3://datalake-demo-pedro/curated/airport_fee/",
    connection_type="s3",
    partitionKeys=["year", "month"],
    compression="snappy",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    transformation_ctx="sink_airport"
)
sink_airport.setCatalogInfo(catalogDatabase="datalake-catalog-taxis", catalogTableName="taxi_airport_fee")
sink_airport.setFormat("glueparquet")
sink_airport.writeFrame(dyf_airport)

## --- Consulta 2: Total Amount por día ---
df_total = df.groupBy(
    year("tpep_pickup_datetime").alias("year"),
    month("tpep_pickup_datetime").alias("month"),
    dayofmonth("tpep_pickup_datetime").alias("day")
).agg(
    sum("total_amount").alias("total_income"),
    count("*").alias("num_trips")
)
dyf_total = DynamicFrame.fromDF(df_total, glueContext, "dyf_total")
sink_total = glueContext.getSink(
    path="s3://datalake-demo-pedro/curated/total_amount/",
    connection_type="s3",
    partitionKeys=["year", "month"],
    compression="snappy",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    transformation_ctx="sink_total"
)
sink_total.setCatalogInfo(catalogDatabase="datalake-catalog-taxis", catalogTableName="taxi_total_amount")
sink_total.setFormat("glueparquet")
sink_total.writeFrame(dyf_total)

## --- Consulta 3: Eficiencia (total_amount / trip_distance) ---
df_efficiency = df.withColumn("efficiency", col("total_amount") / col("trip_distance"))
df_efficiency_summary = df_efficiency.groupBy("pulocationid", "dolocationid", "year", "month").agg(
    round(avg("efficiency"), 2).alias("avg_efficiency"),
    count("*").alias("num_trips")
)
dyf_efficiency = DynamicFrame.fromDF(df_efficiency_summary, glueContext, "dyf_efficiency")
sink_efficiency = glueContext.getSink(
    path="s3://datalake-demo-pedro/curated/efficiency/",
    connection_type="s3",
    partitionKeys=["year", "month"],
    compression="snappy",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    transformation_ctx="sink_efficiency"
)
sink_efficiency.setCatalogInfo(catalogDatabase="datalake-catalog-taxis", catalogTableName="taxi_efficiency")
sink_efficiency.setFormat("glueparquet")
sink_efficiency.writeFrame(dyf_efficiency)

## --- Commit del Job ---
job.commit()
