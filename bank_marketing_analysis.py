# ==============================
# Project 1 : Bank Marketing Analysis using PySpark
# Dataset : Bank Marketing Dataset
# ==============================

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, col


spark = SparkSession.builder \
    .appName("Bank Marketing Analysis") \
    .getOrCreate()


df = spark.read.csv("bank.csv", header=True, inferSchema=True)

print("===== Sample Data =====")
df.show(5)


print("===== Schema =====")
df.printSchema()


print("===== Total Records =====")
print(df.count())


print("===== Average Age =====")
df.select(avg("age")).show()


print("===== Job Distribution =====")
df.groupBy("job") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()


print("===== Marital Status =====")
df.groupBy("marital") \
    .count() \
    .show()


print("===== Education =====")
df.groupBy("education") \
    .count() \
    .show()


print("===== Term Deposit Subscription =====")
df.groupBy("deposit") \
    .count() \
    .show()


print("===== Job vs Subscription =====")
df.groupBy("job","deposit") \
    .count() \
    .orderBy("job") \
    .show()


print("===== Housing Loan vs Subscription =====")
df.groupBy("housing","deposit") \
    .count() \
    .show()


print("===== Personal Loan vs Subscription =====")
df.groupBy("loan","deposit") \
    .count() \
    .show()


print("===== Balance Statistics =====")
df.describe("balance").show()


print("===== Top 10 Highest Balance =====")
df.select("age","job","balance") \
    .orderBy(col("balance").desc()) \
    .show(10)


print("===== Month Distribution =====")
df.groupBy("month") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()


print("===== Call Duration Statistics =====")
df.describe("duration").show()


print("===== Campaign Contact Count =====")
df.groupBy("campaign") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()


spark.stop()
