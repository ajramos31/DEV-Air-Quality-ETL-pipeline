# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "87c0302d-fecc-486b-9d67-943187cfd2a4",
# META       "default_lakehouse_name": "team2_LH",
# META       "default_lakehouse_workspace_id": "246d8a46-54fb-4891-b6c6-a0f96a6a126f",
# META       "known_lakehouses": [
# META         {
# META           "id": "87c0302d-fecc-486b-9d67-943187cfd2a4"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "b746263a-ea14-a08f-4d7a-7532c4448473",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read in bronze date
BRONZE_PATH = "Files/RAW/aqs_dailyByState"

bronze = spark.read.parquet(BRONZE_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

standard_filter = [
    "CO 8-hour 1971",
    "SO2 1-hour 2010",
    "NO2 1-hour 2010",
    "Ozone 8-hour 2015",
    "PM10 24-hour 2006",
    "PM25 24-hour 2012"
]

silver_base = (
    bronze.withColumn("pollutant_standard", F.trim(F.col("pollutant_standard")))
    .filter(F.col("pollutant_standard").isin(standard_filter))
    .drop("pollutant_standard")
    .filter((F.col("validity_indicator") == "Y") &
     (F.col("arithmetic_mean").isNotNull()) & (F.col("aqi").isNotNull()))
    .withColumn("aqi", F.col("aqi").cast("int"))
    .withColumn("first_max_hour", F.col("first_max_hour").cast("int"))
    .withColumn("observation_count", F.col("observation_count").cast("int"))
    .withColumn("poc", F.col("poc").cast("int"))
    .withColumn("site_number", F.col("site_number").cast("int"))
    .withColumn("date_local", F.col("date_local").cast("date"))
    .withColumn("date_of_last_change", F.to_date("date_of_last_change"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(silver_base)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Normalized Tables

# CELL ********************

# Measurement Table
pk_cols = ["state_code","county_code","site_number","parameter_code","poc","date_local"]
silver_daily =(
    silver_base
    .select(
        *pk_cols,
        "method_code","arithmetic_mean","first_max_value","first_max_hour","aqi",
        "observation_count","observation_percent","validity_indicator",
        F.col("units_of_measure"),
        "sample_duration","event_type","date_of_last_change","year","month"
    ).dropDuplicates(pk_cols)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Smaller table
silver_cbsa = (
    silver_base
    .select(F.col("cbsa_code").cast("int"), "cbsa")
    .where(F.col("cbsa_code").isNotNull())
    .dropDuplicates(["cbsa_code"])
)

silver_admin_area = (
    silver_base
    .select(
        "state_code","county_code","state","county"
    )
    .dropDuplicates(["state_code", "county_code"])
)

silver_site = (
    silver_base
    .select(
        "state_code","county_code","site_number",
        "latitude","longitude","datum","local_site_name",
        "site_address","city","cbsa_code"
    )
    .dropDuplicates(["state_code", "county_code", "site_number"])
)

silver_parameter = (
    silver_base
    .select(
        "parameter_code",
        F.col("parameter").cast("string")
    )
    .where(F.col("parameter_code").isNotNull())
    .dropDuplicates(["parameter_code"])
)


silver_method = (
    silver_base
    .select(
        "method_code",
        F.col("method")
    )
    .where(F.col("method_code").isNotNull())
    .dropDuplicates(["method_code"])
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Writing to Silver as Delta Tables

# CELL ********************

'''
silver_cbsa.write.format("delta").mode("overwrite").saveAsTable("silver_cbsa")
silver_admin_area.write.format("delta").mode("overwrite").saveAsTable("silver_admin_area")
silver_site.write.format("delta").mode("overwrite").saveAsTable("silver_site")
silver_method.write.format("delta").mode("overwrite").saveAsTable("silver_method")

# Measurement table 
(silver_daily
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable("silver_daily_measurement")
    )
'''
silver_parameter.write.format("delta").mode("overwrite").saveAsTable("silver_parameter")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
