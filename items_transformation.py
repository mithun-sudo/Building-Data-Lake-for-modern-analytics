from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, udf, to_timestamp, col


def opening_time(timings):
    len_timings = len(timings)
    if timings[0] == '24Hours':
        return '00:00:00'
    if len_timings == 1:
        if '–' not in timings[0]:
            return timings[0]
        else:
            return timings[0].split('–')[0]
    elif len_timings > 1:
        return timings[0].split('–')[0]
    else:
        return ""


def closing_time(timings):
    len_timings = len(timings)
    if timings[0] == '24Hours':
        return '00 00'
    if len_timings == 1:
        if '–' not in timings[0]:
            return ''
        else:
            return timings[0].split('–')[1]
    elif len_timings > 1:
        return timings[len_timings - 1].split('–')[1]
    else:
        return ""


def time_processing(t):
    if 'am' in t:
        t = t.replace('am', '')
        if ':' in t:
            return t + ":00"
        else:
            v = int(t)
            if v == 12:
                return '00:00:00'
            else:
                return str(v) + ":00:00"
    elif 'pm' in t:
        if ':' in t:
            t = t.replace('pm', "")
            l = t.split(':')
            v = l[0]
            s = l[1]
            if v == '12':
                return t + ":00"
            else:
                return str(int(v) + 12) + ":" + s + ":00"
        else:
            v = int(t.replace('pm', ""))
            if v == 12:
                return '12:00:00'
            else:
                return str(v + 12) + ":00:00"
    else:
        return t


opening_time_UDF = udf(lambda z: opening_time(z))
closing_time_UDF = udf(lambda z: closing_time(z))
process_time_UDF = udf(lambda z: time_processing(z))
spark = SparkSession.builder.appName('Items_transformation').getOrCreate()
df = spark.read.parquet(r'C:\Users\mithu\Downloads\items.parquet')
df = df.withColumn("timings", regexp_replace("timings", "[^0-9A-Za-z\.,–:]", ""))
df = df.withColumn("timings", regexp_replace("timings", "Opensat|Today|OpensonSundayat|Openstomorrowat", ""))
df = df.withColumn("timings", regexp_replace("timings", "noon", "pm"))
df = df.withColumn("timings", regexp_replace("timings", "midnight", "am"))
df = df.withColumn("timings", regexp_replace("timings", "midnight", "am"))
df = df.withColumn("timings", split("timings", ","))
df = df.withColumn("opening_time", opening_time_UDF("timings"))
df = df.withColumn("closing_time", closing_time_UDF("timings"))
df = df.withColumn("opening_time", process_time_UDF("opening_time"))
df = df.withColumn("closing_time", process_time_UDF("closing_time"))
df = df.withColumn("opening_time", to_timestamp("opening_time", "H:mm:ss"))
df = df.withColumn("closing_time", to_timestamp("closing_time", "H:mm:ss"))
df = df.withColumn("no_of_reviews", df.no_of_reviews.cast("Integer"))
df = df.withColumn("delivery_rating", df.delivery_rating.cast("Float"))
df = df.withColumn("item_cost", regexp_replace(col("item_cost"), "₹", "").cast("Float"))
restaurant_df = spark.read.parquet(r'C:\Users\mithu\Downloads\tranformation\restaurant_info_final.parquet')
df.createOrReplaceTempView("B")
restaurant_df.createOrReplaceTempView("A")
final_df = spark.sql("""
select distinct A.id, A.name, B.address, B.opening_time, B.closing_time, B.no_of_reviews, B.delivery_rating, A.url, discount_id,
orders_placed, stars, cost_for_one from A inner join B on A.name = B.name
""")
final_df.write.parquet(r'C:\Users\mithu\Downloads\tranformation\restaurant_info_final_final.parquet')
df = df.select("id", "name", "item_name", "item_cost", "item_description", "veg_non_veg")
df.createOrReplaceTempView("B")
items_df = spark.sql("""
select B.id as id, A.id as restaurant_id, item_name, item_cost, item_description, veg_non_veg from B
join A on A.name = B.name where A.id is not null""")
items_df.write.parquet(r'C:\Users\mithu\Downloads\tranformation\items_final.parquet')
restaurant_df = spark.read.parquet(r'C:\Users\mithu\Downloads\tranformation\restaurant_info_final_final.parquet')
item_df = spark.read.parquet(r'C:\Users\mithu\Downloads\tranformation\items_final.parquet')
restaurant_df.printSchema()
restaurant_df.show()
item_df.printSchema()
item_df.show()
df.printSchema()
