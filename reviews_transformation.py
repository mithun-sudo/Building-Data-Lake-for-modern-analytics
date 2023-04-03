from pyspark.sql import SparkSession
from pyspark.sql.functions import hash, col, regexp_replace, split, to_date, udf, when
from pyspark.sql.types import StringType
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
dyf = glue_context.create_dynamic_frame.from_catalog(database='scrap_database', table_name='reviews')
df = dyf.toDF()
df.createOrReplaceTempView('reviews')
restaurant_info_df.createOrReplaceTempView('restaurant')
# Removing restaurant_name from reviews table and replacing it with restaurant_id
df = spark.sql("""
# select B.id, user_name, A.reviews, timestamp, A.ratings, A.type from reviews A inner join restaurant B on
# A.restaurant_name = B.name;
# """)
# Hashing user_name to safe guard user's privacy.
df = df.withColumn("hashed_user_name", hash(col("user_name"))).drop('user_name')
# Casting the column into float data type.
df = df.withColumn("ratings", col("ratings").cast("Float"))
# Using regular expressions to remove html tags from reviews that came along during scraping.
df = df.withColumn("reviews", regexp_replace("reviews", "<[^>]*>", ''))
df = df.withColumn("raw_timestamp", col("timestamp"))
# Following transformations to convert the column into timestamp format.
df = df.withColumn("timestamp", regexp_replace("timestamp", "one", '1'))
df = df.withColumn("timestamp", regexp_replace("timestamp", "yesterday", '1 days ago'))
df = df.withColumn("timestamp", split("timestamp", " "))
df.createOrReplaceTempView('table')
df = spark.sql("""
select *,
case when timestamp[2] = 'ago' then timestamp[1] else NULL end as params,
case when timestamp[2] = 'ago' then timestamp[0] else NULL end as value,
case when timestamp[2] != 'ago' then (
    case timestamp[0]
        WHEN 'Jan' THEN '01'
        WHEN 'Feb' THEN '02'
        WHEN 'Mar' THEN '03'
        WHEN 'Apr' THEN '04'
        WHEN 'May' THEN '05'
        WHEN 'Jun' THEN '06'
        WHEN 'Jul' THEN '07'
        WHEN 'Aug' THEN '08'
        WHEN 'Sep' THEN '09'
        WHEN 'Oct' THEN '10'
        WHEN 'Nov' THEN '11'
        WHEN 'Dec' THEN '12'
       end
) else NULL end as month,
case when timestamp[2] != 'ago' then (timestamp[1]) else NULL end as day,
case when timestamp[2] != 'ago' then timestamp[2] else NULL end as year
from table;
""")
df = df.withColumn("day", regexp_replace("day", ',', ""))
df = df.withColumn("year", col("year").cast("Integer"))
df = df.withColumn("value", col("value").cast("Integer"))
df.createOrReplaceTempView('table')
df = spark.sql("""
select *, case
when timestamp[2] = 'ago' then (
case
    when params = 'days' or params = 'day' then SUBSTRING(DATEADD(day, -value, "2023-02-15"), 1, 10)
    when params = 'hours' or params = 'hour' then "2023-02-15"
    when params = 'months' or params = 'month' then SUBSTRING(DATEADD(month, -value, "2023-02-15"), 1, 10)
    end
)
when timestamp[2] != 'ago' then CONCAT(year, '-', month, '-', day)
end as date from table;
""")
df = df.withColumn("date", to_date("date", "yyyy-MM-dd"))
df = df.select("id", "hashed_user_name", "reviews", "ratings", "type", "date", "raw_timestamp")
df.write.parquet('s3://tranformation-2-zone/reviews_final.parquet')

