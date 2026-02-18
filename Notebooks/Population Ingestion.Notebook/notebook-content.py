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

import requests
from pyspark.sql.functions import col

years = range(2015, 2025)
results = []


url = 'https://api.census.gov/data/2024/acs/acs1?get=NAME,B01003_001E&for=state:*'
r = requests.get(url)
data = r.json()
headers = data[0]

df = spark.createDataFrame(data, headers)
df = df.withColumnRenamed('state', 'State_code')
df = df.withColumnRenamed('B01003_001E', 'Population')
df = df.withColumnRenamed('NAME', 'State')
df = df.where(col('State') != 'NAME')

df.write.format('delta').mode('overwrite').saveAsTable('state_population')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
