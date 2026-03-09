from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# ---------------------------------
# 1. Create Spark Session
# ---------------------------------

spark = SparkSession.builder \
    .appName("Bank Marketing Analysis") \
    .getOrCreate()

# ---------------------------------
# 2. Load Dataset
# ---------------------------------

df = spark.read.csv(
    "bank.csv",
    header=True,
    inferSchema=True
)

print("===== Dataset Schema =====")
df.printSchema()

print("===== Sample Data =====")
df.show(5)

# ---------------------------------
# Analysis 1 : Job vs Deposit
# ---------------------------------

job_analysis = df.filter(col("deposit") == "yes") \
    .groupBy("Job") \
    .agg(count("*").alias("total_deposit")) \
    .orderBy(col("total_deposit").desc())

print("===== Deposit by Job =====")
job_analysis.show()

# ---------------------------------
# Analysis 2 : Average Age
# ---------------------------------

age_analysis = df.filter(col("deposit") == "yes") \
    .agg(avg("Age").alias("average_age"))

print("===== Average Age of Customers who Deposit =====")
age_analysis.show()

# ---------------------------------
# Analysis 3 : Call Duration Impact
# ---------------------------------

duration_analysis = df.groupBy("deposit") \
    .agg(avg("Duration").alias("avg_call_duration"))

print("===== Average Call Duration =====")
duration_analysis.show()

# ---------------------------------
# Analysis 4 : Housing Loan Impact
# ---------------------------------

housing_analysis = df.groupBy("Housing","deposit") \
    .agg(count("*").alias("total_customers"))

print("===== Housing Loan vs Deposit =====")
housing_analysis.show()

# ---------------------------------
# Analysis 5 : Month vs Deposit
# ---------------------------------

month_analysis = df.filter(col("deposit") == "yes") \
    .groupBy("Month") \
    .agg(count("*").alias("total_deposit")) \
    .orderBy(col("total_deposit").desc())

print("===== Deposit by Month =====")
month_analysis.show()

# ---------------------------------
# Analysis 6 : Education vs Deposit
# ---------------------------------

education_analysis = df.groupBy("Education","deposit") \
    .agg(count("*").alias("total_customers"))

print("===== Education vs Deposit =====")
education_analysis.show()

# ---------------------------------
# Conversion Rate
# ---------------------------------

total_customers = df.count()
total_deposit = df.filter(col("deposit") == "yes").count()

print("===== Conversion Rate =====")
print("Total Customers:", total_customers)
print("Total Deposit:", total_deposit)
print("Deposit Rate:", total_deposit/total_customers)

spark.stop()
