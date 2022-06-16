from pyspark.sql import SparkSession
from pyspark.sql import functions as F, window
from pyspark.sql.types import StringType
from time import sleep
def jobA():
    spark = SparkSession.builder.appName("pyspark_jobs").getOrCreate()
    print(spark)
    print("Hello Iguazio")
    sleep(120)
    df = spark.read.option("header", True).csv("v3io://new-rxp/Sales_Records.csv")
    df.printSchema()
    df = df.withColumn('source', F.lit('test-col'))
    df = df.withColumn('flg_blk', F.lit(1))
    df = df.filter(F.col('Region').isNotNull())
    w = window.Window().partitionBy('Country').orderBy(F.col('Order_Date').desc())
    df = df.withColumn('win_test', F.row_number().over(w))
    print("Distinct Count: " + str(df.distinct().count()))
    df.show()