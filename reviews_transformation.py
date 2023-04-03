import time
from calendar import calendar
from datetime import datetime, timedelta, date
import dateutil
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import hash, col, regexp_replace, split, to_date, udf, when
from pyspark.sql.types import StringType


# def reviews_encoding(review):
#     if review:
#         return review.encode('utf-8').decode('utf-8')
#     else:
#         return review


def date_transform(timestamp):
    if timestamp[2] == 'ago':
        if timestamp[1] == 'days' or 'day':
            return str(date(2023, 2, 15) + relativedelta(days=-int(timestamp[0])))
        if timestamp[1] == 'hours' or 'hour':
            return str(date(2023, 2, 15))
        if timestamp[1] == 'month' or 'months':
            return str(date(2023, 2, 15) + relativedelta(months=-int(timestamp[0])))
    if timestamp[2] != 'ago':
        if timestamp[0] == 'Jan':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'Feb':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'Mar':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'Apr':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'May':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'Jun':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'Jul':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'Aug':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'Sep':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'Oct':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'Nov':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))
        elif timestamp[0] == 'Dec':
            return str(date(int(timestamp[2]), 1, int(timestamp[1].replace(',', ""))))


start = time.time()
dateUDF = udf(lambda z: date_transform(z))
# encode_udf = udf(lambda z: reviews_encoding(z))
spark = SparkSession.builder.appName('Review_transformation').getOrCreate()
df = spark.read.parquet(r'C:\Users\mithu\Downloads\reviews.parquet')
# restaurant_info_df = spark.read.parquet(r'C:\Users\mithu\Downloads\tranformation\restaurant_info_final.parquet')
# df.createOrReplaceTempView('reviews')
# restaurant_info_df.createOrReplaceTempView('restaurant')
# df = spark.sql("""
# select B.id, user_name, A.reviews, timestamp, A.ratings, A.type from reviews A inner join restaurant B on
# A.restaurant_name = B.name;
# """)
# df = df.withColumn("hashed_user_name", hash(col("user_name"))).drop('user_name')
# df = df.withColumn("ratings", col("ratings").cast("Float"))
# df = df.withColumn("reviews", regexp_replace("reviews", "<[^>]*>", ''))
# df = df.withColumn("raw_timestamp", col("timestamp"))
# df = df.withColumn("timestamp", regexp_replace("timestamp", "one", '1'))
# df = df.withColumn("timestamp", regexp_replace("timestamp", "yesterday", '1 days ago'))
# df = df.withColumn("timestamp", split("timestamp", " "))
# # df = df.withColumn("timestamp", dateUDF("timestamp"))
# # df = df.withColumn('reviews', when(col("reviews") != 'null', encode_udf(col("reviews"))))
# df.createOrReplaceTempView('table')
# df = spark.sql("""
# select *,
# case when timestamp[2] = 'ago' then timestamp[1] else NULL end as params,
# case when timestamp[2] = 'ago' then timestamp[0] else NULL end as value,
# case when timestamp[2] != 'ago' then (
#     case timestamp[0]
#         WHEN 'Jan' THEN '01'
#         WHEN 'Feb' THEN '02'
#         WHEN 'Mar' THEN '03'
#         WHEN 'Apr' THEN '04'
#         WHEN 'May' THEN '05'
#         WHEN 'Jun' THEN '06'
#         WHEN 'Jul' THEN '07'
#         WHEN 'Aug' THEN '08'
#         WHEN 'Sep' THEN '09'
#         WHEN 'Oct' THEN '10'
#         WHEN 'Nov' THEN '11'
#         WHEN 'Dec' THEN '12'
#        end
# ) else NULL end as month,
# case when timestamp[2] != 'ago' then (timestamp[1]) else NULL end as day,
# case when timestamp[2] != 'ago' then timestamp[2] else NULL end as year
# from table;
# """)
# df = df.withColumn("day", regexp_replace("day", ',', ""))
# df = df.withColumn("year", col("year").cast("Integer"))
# df = df.withColumn("value", col("value").cast("Integer"))
# df.createOrReplaceTempView('table')
# df = spark.sql("""
# select *, case
# when timestamp[2] = 'ago' then (
# case
#     when params = 'days' or params = 'day' then SUBSTRING(DATEADD(day, -value, "2023-02-15"), 1, 10)
#     when params = 'hours' or params = 'hour' then "2023-02-15"
#     when params = 'months' or params = 'month' then SUBSTRING(DATEADD(month, -value, "2023-02-15"), 1, 10)
#     end
# )
# when timestamp[2] != 'ago' then CONCAT(year, '-', month, '-', day)
# end as date from table;
# """)
# df = df.withColumn("date", to_date("date", "yyyy-MM-dd"))
# df = df.select("id", "hashed_user_name", "reviews", "ratings", "type", "date", "raw_timestamp")
# df = df.filter(col("id") == 657)
df.printSchema()
# df.show()
# df.write.parquet(r'C:\Users\mithu\Downloads\tranformation\reviews_final.parquet')

