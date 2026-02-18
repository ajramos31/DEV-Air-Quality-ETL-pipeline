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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "9ab273fb-d562-af3b-4d1f-3e988abb56af",
# META       "known_warehouses": [
# META         {
# META           "id": "9ab273fb-d562-af3b-4d1f-3e988abb56af",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_cbsa  = spark.table("silver_cbsa")
silver_admin = spark.table("silver_admin_area")
silver_site  = spark.table("silver_site")
silver_param = spark.table("silver_parameter")
silver_method= spark.table("silver_method")
silver_daily = spark.table("silver_daily_measurement")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dimension Tables

# CELL ********************

# Dim Date
dim_date = (
    silver_daily
    .select(F.col("date_local").alias("date"))
    .where(F.col("date").isNotNull())
    .dropDuplicates(["date"])
    .withColumn("year", F.year("date").cast("int"))
    .withColumn("month", F.month("date").cast("int"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("day", F.dayofmonth("date").cast("int"))
    .withColumn("day_of_week", F.dayofweek("date").cast("int"))  # 1=Mon..7=Sun
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("quarter", F.quarter("date").cast("int"))
    .withColumn("is_weekend", F.col("day_of_week").isin([6,7]))
    .select("date","year","month","month_name","day","day_of_week","day_name","quarter","is_weekend")
)
dim_date_window = Window.orderBy("date")
dim_date = dim_date.withColumn("date_key", F.row_number().over(dim_date_window))

# Dim Parameter
unit_by_param = (
    silver_daily
    .select("parameter_code", F.col("units_of_measure").alias("unit_of_measurement"))
    .where(F.col("parameter_code").isNotNull())
    .where(F.col("units_of_measure").isNotNull())
    .dropDuplicates(["parameter_code", "unit_of_measurement"])
)

dim_parameter = (
    silver_param.alias("p")
    .join(unit_by_param.alias("u"), on="parameter_code", how="left")
    .select(
        "parameter_code",
        F.col("parameter").alias("parameter_name"),
        F.col("unit_of_measurement").alias("unit_of_measurement")
    )
    .dropDuplicates(["parameter_code"])
)
dim_parameter = dim_parameter.withColumn("category",
    F.when(F.col("parameter_code").isin("88101", "81102"), "Particulate Matter")
     .otherwise("Gas")
)
dim_parameter_window = Window.orderBy("parameter_code")
dim_parameter = dim_parameter.withColumn("parameter_key", F.row_number().over(dim_parameter_window))

# Dim Method
dim_method = silver_method.withColumnRenamed("method","method_name")
dim_method_window = Window.orderBy("method_code")
dim_method = dim_method.withColumn("method_key", F.row_number().over(dim_method_window))

# Dim Location
region_col = (
    F.when(F.col("state_code").isin(
        "09","23","25","33","34","36","42","44","50"
    ), "Northeast")

    .when(F.col("state_code").isin(
        "17","18","19","20","26","27","29","31","38","39","46","55"
    ), "Midwest")

    .when(F.col("state_code").isin(
        "01","05","10","11","12","13","21","22","24","28",
        "37","40","45","47","48","51","54"
    ), "South")

    .when(F.col("state_code").isin(
        "02","04","06","08","15","16","30","32","35","41","49","53","56"
    ), "West")

    .otherwise("Unknown")
)

state_population = (
    spark.table("state_population")
    .withColumnRenamed("State_code", "state_code")
    .withColumnRenamed("Population", "population")
    .drop("State")
)

dim_location = (
    silver_site.select(
        "state_code",
        "county_code",
        "site_number",
        "latitude",
        "longitude",
        "city",
        "cbsa_code"
    ).alias("s")
    .join(silver_admin.alias("a"),
        on=[F.col("s.state_code")==F.col("a.state_code"),
            F.col("s.county_code")==F.col("a.county_code")],how="left")
    .join(silver_cbsa.alias("c"),
        on=F.col("s.cbsa_code")==F.col("c.cbsa_code"),how="left"
    )
    .join(state_population.alias("sp"), on=F.col("s.state_code")==F.col("sp.state_code"), how="left")
    .select(
         F.col("s.state_code").alias("state_code"),
         F.col("s.county_code").alias("county_code"),
         F.col("s.site_number").alias("site_number"),
         F.col("a.state").alias("state_name"),
         F.col("a.county").alias("county_name"),
         F.col("s.city").alias("city"),
         F.col("s.cbsa_code").alias("cbsa_code"),
         F.col("c.cbsa").alias("cbsa_name"),
         F.col("s.latitude").alias("latitude"),
         F.col("s.longitude").alias("longitude"),
         F.col("sp.population").cast("int").alias("population"),
     )
)

dim_location = dim_location.withColumn("region", region_col)
dim_location_window = Window.orderBy("state_code", "county_code", "site_number")
dim_location = dim_location.withColumn("location_key", F.row_number().over(dim_location_window))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dim_parameter)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fact Table

# CELL ********************

fact_daily = (
    silver_daily.alias("f")
    .join(dim_date.alias("d"), F.col("f.date_local")==F.col("d.date"), "left")
    .join(dim_location.alias("l"),
          on=[F.col("f.state_code")==F.col("l.state_code"),
              F.col("f.county_code")==F.col("l.county_code"),
              F.col("f.site_number")==F.col("l.site_number")],
          how="left")
    .join(dim_parameter.alias("p"), F.col("f.parameter_code")==F.col("p.parameter_code"), "left")
    .join(dim_method.alias("m"), F.col("f.method_code")==F.col("m.method_code"), "left")
    .withColumn(
        "aqi_category",
        F.when(F.col("f.aqi").isNull(), None)
         .when(F.col("f.aqi") <= 50, "Good")
         .when(F.col("f.aqi") <= 100, "Moderate")
         .when(F.col("f.aqi") <= 150, "Unhealthy for Sensitive Groups")
         .when(F.col("f.aqi") <= 200, "Unhealthy")
         .when(F.col("f.aqi") <= 300, "Very Unhealthy")
         .otherwise("Hazardous")
    )
    .select(
        F.col("d.date_key").alias("date_key"),
        F.col("l.location_key").alias("location_key"),
        F.col("p.parameter_key").alias("parameter_key"),
        F.col("f.poc").alias("poc"),
        F.col("m.method_key").alias("method_key"),

        F.col("f.arithmetic_mean").alias("arithmetic_mean"),
        F.col("f.first_max_value").alias("first_max_value"),
        F.col("f.first_max_hour").alias("first_max_hour"),
        F.col("f.aqi").alias("aqi"),
        F.col("f.observation_count").alias("observation_count"),
        F.col("f.observation_percent").alias("observation_percent"),

        F.col("aqi_category").alias("aqi_category"),
    )
)
fact_daily = fact_daily.withColumn("exceeds_standard", F.col("aqi") > 100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to LakeHouse

# CELL ********************

# Write to Lakehouse, then COPY INTO Warehouse
'''
dim_date.write.format("delta").mode("overwrite").saveAsTable("dim_date")
dim_location.write.format("delta").mode("overwrite").saveAsTable("dim_location")

dim_method.write.format("delta").mode("overwrite").saveAsTable("dim_method")
fact_daily.write.format("delta").mode("overwrite").saveAsTable("fact_daily_air_quality")
'''
dim_parameter.write.format("delta").mode("overwrite").saveAsTable("dim_parameter")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
