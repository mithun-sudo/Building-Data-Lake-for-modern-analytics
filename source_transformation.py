from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, expr, regexp_replace, explode

spark = SparkSession.builder.appName('Source_processing').getOrCreate()

df = spark.read.parquet(r'C:\Users\mithu\Downloads\source.parquet')
df = df.select("*", split(col("cost"), ' ')[0].alias("cost_for_one")).drop("cost")
df = df.withColumn("cost_for_one", expr('SUBSTRING(cost_for_one, 2, length(cost_for_one))'))
df = df.withColumn("cost_for_one", col("cost_for_one").cast("Integer"))


df = df.withColumn("orders_placed", regexp_replace("orders_placed", "[^0-9]+", ' '))
df = df.withColumn("orders_placed", col("orders_placed").cast("Integer"))

df = df.withColumn("stars", regexp_replace("stars", "[^0-9.]", ' '))
df = df.withColumn("stars", col("stars").cast("Float"))
df = df.select("id", "name", "url", "discount", "orders_placed", "stars", "cost_for_one")
print('Writing')
df.write.parquet(r'C:\Users\mithu\Downloads\tranformation\restaurant_info.parquet')
df.printSchema()
df = spark.read.parquet(r'C:\Users\mithu\Downloads\source.parquet')
df.createOrReplaceTempView('table')
discount_df = spark.sql("""with temp as (SELECT distinct discount, case when substring(discount, length(discount) - 2,
length(discount)) = 'min' then 0 else 1 end as flag FROM table) select row_number() over (order by discount) as id,
discount from temp where flag = 1;""")
discount_df.write.parquet(r'C:\Users\mithu\Downloads\tranformation\discount.parquet')
df = spark.read.parquet(r'C:\Users\mithu\Downloads\tranformation\restaurant_info.parquet')
discount_df = spark.read.parquet(r'C:\Users\mithu\Downloads\tranformation\discount.parquet')
df.createOrReplaceTempView('restaurant_info')
discount_df.createOrReplaceTempView('discount_table')
df = spark.sql("""select A.id, A.name, A.url, B.id as discount_id, A.orders_placed, A.stars, A.cost_for_one from
restaurant_info A left join discount_table B on A.discount = B.discount;""")
df.write.parquet(r'C:\Users\mithu\Downloads\tranformation\restaurant_info_final.parquet')
df = spark.read.parquet(r'C:\Users\mithu\Downloads\source.parquet')
df.createOrReplaceTempView('restaurant_info')
df = df.select(col("id").alias("restaurant_id"), explode(split(col("categories"), ', ')). alias("categories"))
df.write.parquet(r'C:\Users\mithu\Downloads\tranformation\categories.parquet')
df.show()
df.printSchema()
