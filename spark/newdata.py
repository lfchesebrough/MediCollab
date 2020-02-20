##spark file

##spark file
##read data from s3

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
import boto3
# set up spark SparkContext

spark = SparkSession.builder.appName("CAREGRAPH").getOrCreate()
sqlContext = SQLContext(sparkSession=spark, sparkContext = spark.sparkContext)


# dg2016 = sqlContext.read.csv('s3a://coordinated-care-data/bucket/DocGraph_Hop_Teaming_2016.csv', header = True)
# dg2014 = sqlContext.read.csv('s3a://coordinated-care-data/test/DocGraph_Hop_Teaming_2014_CC/DocGraph_Hop_Teaming_2014.csv', header = True)
# dg2015 = sqlContext.read.csv('s3a://coordinated-care-data/test/DocGraph_Hop_Teaming_2015_CC/DocGraph_Hop_Teaming_2015.csv', header = True)
#taxonomy = sqlContext.read.csv('s3a://coordinated-care-data/test/DocGraph_Hop_Teaming_2016.csv', header = True)
#nppesdata = sqlContext.read.csv('s3a://coordinated-care-data/nppes/npidata_pfile_20050523-20200112.csv', header = True)
# util = sqlContext.read.format('csv').option("header", "true").option("delimiter", "\t").load('s3://coordinated-care-data/bucket/util2017.txt', header = True)

#csvs
dg = sqlContext.read.csv('s3a://coordinated-care-data/bucket/DocGraph_Hop_Teaming_2016.csv', header = True)
mips = sqlContext.read.csv('s3://coordinated-care-data/bucket/mips.csv', header = True)
nationalupdate = sqlContext.read.csv('s3://coordinated-care-data/bucket/nationalupdate.csv', header = True)
utilization = sqlContext.read.format('csv').option("header", "true").option("delimiter", "\t").load('s3://coordinated-care-data/bucket/util2017.txt', header = True)

#txt file



# MAKE TEMPORARY SQL TABLES FROM IMPORTED DATA
dg.registerTempTable("dg")
mips.registerTempTable("mips")
utilization.registerTempTable("utilization")

dg.printSchema()
mips.printSchema()
utilization.printSchema()

# left_join = ta.join(tb, ta.name == tb.name,how='left')
#
# relationships = dg.join(broadcast(utilization))




# Create updated relationship table, require they are in the
# rel = sqlContext.sql("""
# select
# 'REFERRED_TO' as Type,
# from_npi,
# to_npi,
# cast(patient_count as int) patient_count,
# cast(transaction_count as int) transaction_count,
# cast(average_day_wait as int) average_day_wait,
# cast(std_day_wait as int) std_day_wait
# from dg
# where from_npi in
# (select NPI from util)
# or to_npi in (select NPI from util)
# """)

# Reformat column headers of node table for Neo4j
# docs = docs.withColumnRenamed("NPI", "Doctor:ID")\
#     .withColumnRenamed("PROVIDER_TYPE")\
#     .withColumnRenamed("TOTAL_SERVICES", "Total_Services:int")\
#     .withColumnRenamed("NUMBER_OF_HCPCS","NUMBER_OF_HCPCS:int")\
#     .withColumnRenamed("TOTAL_UNIQUE_BENES","TOTAL_UNIQUE_BENES:int")\
#     .withColumnRenamed("TOTAL_SUBMITTED_CHRG_AMT","TOTAL_SUBMITTED_CHRG_AMT:int")\
#     .withColumnRenamed("TOTAL_MEDICARE_ALLOWED_AMT","TOTAL_MEDICARE_ALLOWED_AMT:int")\
#     .withColumnRenamed("BENEFICIARY_AVERAGE_RISK_SCORE","BENEFICIARY_AVERAGE_RISK_SCORE:int")\
#     .withColumnRenamed("BENEFICIARY_CC_DIAB_PERCENT","BENEFICIARY_CC_DIAB_PERCENT:int")
#
# # Reformat column headers of relationship table for Neo4j
# rel = rel.withColumnRenamed("Type",":TYPE")\
#     .withColumnRenamed("from_npi", ":START_ID")\
#     .withColumnRenamed("to_npi", ":END_ID")\
#     .withColumnRenamed("patient_count", "patient_count:int")\
#     .withColumnRenamed("transaction_count", "transaction_count:int")\
#     .withColumnRenamed("average_day_wait", "average_day_wait:int")\
#     .withColumnRenamed("std_day_wait", "std_day_wait:int")\
#
# # WRITE DATA to csv in s3
# # REPARTITION?
# #
# rel.repartition(1).write.option('header', 'true').mode('append').csv('s3a://coordinated-care-data/neo4j')
# docs.repartition(1).write.option('header', 'true').mode('append').csv('s3a://coordinated-care-data/neo4j')
