from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, expr, regexp_replace, explode
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Boiler plate from glue interactive notebook.
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
dyf = glue_context.create_dynamic_frame.from_catalog(database='database_name', table_name='table_name')
df = dyf.toDF()
# Extracting the cost from a string and casting it into integer.
df = df.select("*", split(col("cost"), ' ')[0].alias("cost_for_one")).drop("cost")
df = df.withColumn("cost_for_one", expr('SUBSTRING(cost_for_one, 2, length(cost_for_one))'))
df = df.withColumn("cost_for_one", col("cost_for_one").cast("Integer"))
# Using regular expression removing all characters that are not numerals and casting it as integer.
df = df.withColumn("orders_placed", regexp_replace("orders_placed", "[^0-9]+", ' '))
df = df.withColumn("orders_placed", col("orders_placed").cast("Integer"))
# Using regular expression removing all characters that are not numerals and casting it as float.
df = df.withColumn("stars", regexp_replace("stars", "[^0-9.]", ' '))
df = df.withColumn("stars", col("stars").cast("Float"))
# Selecting the columns required columns that are needed for future transformations.
restaurant_df = df.select("id", "name", "url", "discount", "orders_placed", "stars", "cost_for_one")
# Creating a new table that contains discount description and discount id. Creating a temporary view for spark SQL.
df.createOrReplaceTempView('table')
discount_df = spark.sql("""with temp as 
(SELECT distinct discount, case when substring(discount, length(discount) - 2,
length(discount)) = 'min' then 0 else 1 end as flag FROM table) 
select row_number() over (order by discount) as id, discount from temp where flag = 1;""")
discount_df.cache()
discount_df.write.parquet('s3://tranformation-2-zone/discount.parquet')
restaurant_df.createOrReplaceTempView('restaurant_info')
discount_df.createOrReplaceTempView('discount_table')
# Replacing the column discount with discount id.
restaurant_df = spark.sql("""
select 
A.id, A.name, A.url, B.id as discount_id, A.orders_placed, A.stars, A.cost_for_one 
from
restaurant_info A 
left join 
discount_table B on A.discount = B.discount;""")
restaurant_df.write.parquet('s3://tranformation-2-zone/restaurant_info_final.parquet')
# Creating a new table categories which relates restaurant id with multiple categories.
df = df.select(col("id").alias("restaurant_id"), explode(split(col("categories"), ', ')). alias("categories"))
df.write.parquet('s3://tranformation-2-zone/categories.parquet')


