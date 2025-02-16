from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Cluster Execution").getOrCreate()

df = spark.range(10)

df.write.format("csv").option("header", True).save("data/output/range.csv")

spark.stop()


# Commad: ./bin/spark-submit --master local[*] --num-executors 3 --executor-cores 2 --executor-memory 500MB /home/rpal_umassd_edu/Spark/Spark_submit.py 