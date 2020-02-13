##spark file

##spark file
##read data from s3

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
import boto3
# set up spark SparkContext

spark = SparkSession.builder.appName("CAREGRAPH").getOrCreate()
sqlContext = SQLContext(sparkSession=spark, sparkContext = spark.sparkContext)


dg2016 = sqlContext.read.csv('s3a://coordinated-care-data/test/DocGraph_Hop_Teaming_2016.csv', header = True)
# dg2014 = sqlContext.read.csv('s3a://coordinated-care-data/test/DocGraph_Hop_Teaming_2014_CC/DocGraph_Hop_Teaming_2014.csv', header = True)
# dg2015 = sqlContext.read.csv('s3a://coordinated-care-data/test/DocGraph_Hop_Teaming_2015_CC/DocGraph_Hop_Teaming_2015.csv', header = True)
#taxonomy = sqlContext.read.csv('s3a://coordinated-care-data/test/DocGraph_Hop_Teaming_2016.csv', header = True)
#nppesdata = sqlContext.read.csv('s3a://coordinated-care-data/nppes/npidata_pfile_20050523-20200112.csv', header = True)
util = sqlContext.read.format('csv').option("header", "true").option("delimiter", "\t").load('s3a://coordinated-care-data/test/Medicare_Physician_and_Other_Supplier_NPI_Aggregate_CY2016.txt', header = True)

# MAKE TEMPORARY SQL TABLES FROM IMPORTED DATA
# dg2014.registerTempTable("dg2014")
# dg2015.registerTempTable("dg2015")
dg2016.registerTempTable("dg2016")
util.registerTempTable("util")
#nppesdata.registerTempTable("nppes")
#util.show(n = 5, vertical = True)
#nppesheader.show(5)

#NPPES COLUMNS:
# NPI, Entity Type Code, Provider Last Name (Legal Name), Provider First Name, Provider Credential Text, Provider Business Mailing Address Postal Code, NPI Deactivation Date, Healthcare Provider Taxonomy Code_1, Is Sole Proprietor,

#UTILIZATION COLUMNS:
# NPI, PROVIDER_TYPE, NPPES_PROVIDER_LAST_ORG_NAME, NPPES_PROVIDER_FIRST_NAME, TOTAL_SERVICES, NUMBER_OF_HCPCS, TOTAL_UNIQUE_BENES, TOTAL_SUBMITTED_CHRG_AMT, TOTAL_MEDICARE_ALLOWED_AMT, BENEFICIARY_AVERAGE_RISK_SCORE, BENEFICIARY_CC_DIAB_PERCENT

# DocGraph columns
#from_npi|    to_npi|patient_count|transaction_count|average_day_wait|std_day_wait
#RUN A SQL QUERY ON THE TABLE

# Merge 3 years of DocGraph, average measures, require relationship in 2016
# dg = sqlContext.sql("""
# select
# from_npi,
# to_npi,
# cast(avg(patient_count) as int) patient_count,
# cast(avg(transaction_count) as int) transaction_count,
# cast(avg(average_day_wait) as int) average_day_wait,
# cast(avg(std_day_wait) as int) std_day_wait
# from (select *
# from dg2016
#
# union
#
# select *
# from dg2015
#
# union
#
# select *
# from dg2014
#      ) a
# where from_npi in (select from_npi from dg2016)
# and to_npi in (select to_npi from dg2016)
# group by from_npi, to_npi
# """)

dg = sqlContext.sql("""
select
from_npi,
to_npi,
cast(patient_count as int) patient_count,
cast(transaction_count as int) transaction_count,
cast(average_day_wait as int) average_day_wait
,cast(std_day_wait as int) std_day_wait
from dg2016
""")


dg.registerTempTable("dg")

# Select desired columns from medicare utilization data, must be in docgraph table
docs = sqlContext.sql("""
select NPI,
PROVIDER_TYPE,
NPPES_PROVIDER_LAST_ORG_NAME,
NPPES_ENTITY_CODE,
NPPES_PROVIDER_FIRST_NAME,
cast(TOTAL_SERVICES as int) TOTAL_SERVICES,
cast(NUMBER_OF_HCPCS as int) NUMBER_OF_HCPCS,
cast(TOTAL_UNIQUE_BENES as int) TOTAL_UNIQUE_BENES,
cast(TOTAL_SUBMITTED_CHRG_AMT as int) TOTAL_SUBMITTED_CHRG_AMT,
cast(TOTAL_MEDICARE_ALLOWED_AMT as int) TOTAL_MEDICARE_ALLOWED_AMT,
cast(BENEFICIARY_AVERAGE_RISK_SCORE as int) BENEFICIARY_AVERAGE_RISK_SCORE,
cast(BENEFICIARY_CC_DIAB_PERCENT as int) BENEFICIARY_CC_DIAB_PERCENT
from util
where nppes_entity_code = 'I'
and (npi in (select from_npi from dg)
OR npi in (select to_npi from dg))
""")

docs.registerTempTable("util")


#create updated relationship table, require they are in the
rel = sqlContext.sql("""
select
'REFERRED_TO' as Type,
from_npi,
to_npi,
cast(patient_count as int) patient_count,
cast(transaction_count as int) transaction_count,
cast(average_day_wait as int) average_day_wait,
cast(std_day_wait as int) std_day_wait
from dg
where from_npi in
(select NPI from util)
or to_npi in (select NPI from util)
""")

#
docs = docs.withColumnRenamed("NPI", "Doctor:ID")\
    .withColumnRenamed("PROVIDER_TYPE", ":LABEL")\
    .withColumnRenamed("TOTAL_SERVICES", "Total_Services:int")\
    .withColumnRenamed("NUMBER_OF_HCPCS","NUMBER_OF_HCPCS:int")\
    .withColumnRenamed("TOTAL_UNIQUE_BENES","TOTAL_UNIQUE_BENES:int")\
    .withColumnRenamed("TOTAL_SUBMITTED_CHRG_AMT","TOTAL_SUBMITTED_CHRG_AMT:int")\
    .withColumnRenamed("TOTAL_MEDICARE_ALLOWED_AMT","TOTAL_MEDICARE_ALLOWED_AMT:int")\
    .withColumnRenamed("BENEFICIARY_AVERAGE_RISK_SCORE","BENEFICIARY_AVERAGE_RISK_SCORE:int")\
    .withColumnRenamed("BENEFICIARY_CC_DIAB_PERCENT","BENEFICIARY_CC_DIAB_PERCENT:int")

#
rel = rel.withColumnRenamed("Type",":TYPE")\
    .withColumnRenamed("from_npi", ":START_ID")\
    .withColumnRenamed("to_npi", ":END_ID")\
    .withColumnRenamed("patient_count", "patient_count:int")\
    .withColumnRenamed("transaction_count", "transaction_count:int")\
    .withColumnRenamed("average_day_wait", "average_day_wait:int")\
    .withColumnRenamed("std_day_wait", "std_day_wait:int")\

# WRITE DATA to csv in s3
#REPARTITION?
#
rel.repartition(1).write.option('header', 'true').mode('append').csv('s3a://coordinated-care-data/relationships')
docs.repartition(1).write.option('header', 'true').mode('append').csv('s3a://coordinated-care-data/nodes')
