from pyspark.sql import SparkSession
from pyspark.sql.functions import col,concat,lit,substring,when
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType,DoubleType


spark = SparkSession.builder.appName("Testtest").config("spark.driver.memory", "5g").getOrCreate()

## Setting the S3 configuration 
spark_context = spark.sparkContext
spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "B2JDY11NHXLI77PHSX4D") 
spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "XBbgD4eM8Su2B7AZVyTe4hKY2IR1Oz05QYYEvCaD") 
spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark_context._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint","http://s3-rook-ceph.apps.okdpres.alerant.org.uk")


## Define Schema
yellow_cub_Schema = StructType([
    StructField("vendorid", IntegerType(), True),        
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
	StructField("passenger_count", IntegerType(), True),
	StructField("trip_distance", DoubleType(), True),
	StructField("ratecodeid", IntegerType(), True),
	StructField("store_and_fwd_flag", DoubleType(), True),
	StructField("pulocationid", IntegerType(), True),
	StructField("dolocationid", IntegerType(), True),
	StructField("payment_type", IntegerType(), True),
	StructField("fare_amount", DoubleType(), True),
	StructField("extra", DoubleType(), True),
	StructField("mta_tax", DoubleType(), True),
	StructField("tip_amount", DoubleType(), True),
	StructField("tolls_amount", DoubleType(), True),	
	StructField("improvement_surcharge", DoubleType(), True),
	StructField("total_amount", DoubleType(), True),
	StructField("congestion_surcharge", DoubleType(), True)])


df=spark.read.option("delimiter", ",").option("header", "true").schema(yellow_cub_Schema).csv("s3a://spark/test_data/trip_data_3.csv")
df.printSchema()

df_filtered = df.filter(df.trip_distance >=1.0)
df_transform= df_filtered.withColumn("pickup_date",substring(df_filtered.tpep_pickup_datetime,1,10)).withColumn("dropoff_date",substring(df_filtered.tpep_dropoff_datetime,1,10))
df_selected=df_transform.select(col("pickup_date"),col("dropoff_date"),col("passenger_count"),col("trip_distance"),col("pulocationid"),col("dolocationid"),col("payment_type"),col("total_amount"),col("tip_amount"))
df_selected.createOrReplaceTempView("Yellow_cab")

df_sql= spark.sql("SELECT pickup_date,dropoff_date,passenger_count,avg(trip_distance) as avg_distance,sum(total_amount) as sum_total_amt, avg(tip_amount) as avg_tip    FROM Yellow_cab group by pickup_date,dropoff_date,passenger_count")
df_sql.show(10)

df_sql.coalesce(1).write.format('csv').mode('overwrite').option("header", "true").save("s3a://spark/test_data/trip_aggr_data.csv")
