##spark file

##spark file
##read data from s3

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
import boto3
# set up spark SparkContext

spark = SparkSession.builder.appName("CAREGRAPH").getOrCreate()
sqlContext = SQLContext(sparkSession=spark, sparkContext = spark.sparkContext)


#csvs
dg = sqlContext.read.csv('s3a://coordinated-care-data/bucket/DocGraph_Hop_Teaming_2017.csv', header = True)
mips = sqlContext.read.csv('s3a://coordinated-care-data/bucket/mips.csv', header = True)
utilization= sqlContext.read.format('csv').option("header", "true").option("delimiter", "\t").load('s3a://coordinated-care-data/bucket/util2017.txt', header = True)
# Make spark dataframes from imported CSVs
dg.registerTempTable("dg")
mips.registerTempTable("mips")
utilization.registerTempTable("utilization")

# Check the Schema
# dg.printSchema()
# mips.printSchema()
# utilizationization.printSchema()

# Create relationship table, require they are in utilization data
docs = sqlContext.sql("""
select /*+ BROADCAST(a) */ distinct
'Doctor' as Label,
a.NPI as NPI,
PROVIDER_TYPE as Specialty,
NPPES_PROVIDER_LAST_ORG_NAME as Last_Name,
NPPES_PROVIDER_FIRST_NAME as First_Name,
cast(cast(TOTAL_MEDICARE_ALLOWED_AMT as int) / cast(TOTAL_UNIQUE_BENES as int)*.2 as int) as Cost_Per_Patient,
cast(b.`Final Mips Score` as int) as Quality_Score,
left(nppes_provider_zip, 5) as Zip_Code

from utilization a
left join mips b on a.npi = b.npi
where nppes_entity_code = 'I'
and a.npi in (select from_npi from dg)
or a.npi in (select to_npi from dg)
""")

rel = sqlContext.sql("""
select distinct
'REFERRED_TO' as Type,
from_npi,
to_npi,
cast(patient_count as int) patient_count,
cast(average_day_wait as int) average_day_wait
from dg
where from_npi in (select npi from utilization)
or to_npi in (select npi from utilization)
""")



# Create identity table, require all NPIs are in the relationship table
# and left join MIPS quality score

# Reformat column headers of node table for Neo4j
docs = docs.withColumnRenamed("NPI", "NPI:ID")\
    .withColumnRenamed("Cost_Per_Patient", "Cost_Per_Patient:int")\
    .withColumnRenamed("Label", ":LABEL")\
    .withColumnRenamed("Quality_Score", "Quality_Score:int")\

#
# # Reformat column headers of relationship table for Neo4j
rel = rel.withColumnRenamed("Type",":TYPE")\
    .withColumnRenamed("from_npi", ":START_ID")\
    .withColumnRenamed("to_npi", ":END_ID")\
    .withColumnRenamed("patient_count", "patient_count:int")\
    .withColumnRenamed("average_day_wait", "average_day_wait:int")\
#
# # WRITE DATA to csv in s3
# #
# #

rel.repartition(1).write.option('header', 'true').mode('append').csv('s3a://coordinated-care-data/neonow')
docs.repartition(1).write.option('header', 'true').mode('append').csv('s3a://coordinated-care-data/neonow')
