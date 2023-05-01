import pytest
from pyspark.context import SparkContext
from pyspark.sql.functions import col, unix_timestamp, from_unixtime, date_format, concat, concat_ws, lit, first, last
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
import sys
sys.path.insert(1, 'src')
# import sample

from pyspark.conf import SparkConf

# Manually Pass Args
sys.argv.append('--JOB_NAME')
sys.argv.append('test_iceberg')
sys.argv.append('--iceberg_job_catalog_warehouse')
sys.argv.append('s3://aws-poc-glue/boto/iceberg/')

# Spark Configuration
# args = getResolvedOptions(sys.argv, ['JOB_NAME', 'iceberg_job_catalog_warehouse'])
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'iceberg_job_catalog_warehouse'])
conf = SparkConf()

## Please make sure to pass runtime argument --iceberg_job_catalog_warehouse with value as the S3 path 
conf.set("spark.sql.catalog.job_catalog.warehouse", args['iceberg_job_catalog_warehouse'])
conf.set("spark.sql.catalog.job_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.job_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.job_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
conf.set("spark.sql.iceberg.handle-timestamp-without-timezone","true")

# Init Job
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


"""
Fetching the Dealer DataFrame
"""
# # Sample from Data Catalog
# df_dealer = glueContext.create_data_frame.from_catalog(
#     database="boto3",
#     table_name="dealer_iceberg"
# )

# # Filter the Dealer DataFrame for Relevant columns
# df_dealer = df_dealer.select(col("id").alias("dealerid"), "name")
# df_dealer.show()

"""
Fetching all required data into DataFrames
"""
# Fetch from Data Catalog
df_message = glueContext.create_data_frame.from_catalog(
    database="boto3",
    table_name="message_iceberg"
)

df_dag = glueContext.create_data_frame.from_catalog(
    database="boto3",
    table_name="dealer_associate_group_iceberg"
)

df_dagmem = glueContext.create_data_frame.from_catalog(
    database="boto3",
    table_name="dealer_associate_group_member_iceberg"
)

# Filter the Dealer DataFrame for Relevant columns
df_message = df_message.filter(col("protocol").isin("E","X") & (col("ismanual")=="1") & col("messagetype").isin("I","S"))\
    .select("id",
            "dealerid",
            "messagetype",
            "protocol",
            "customerid",
            "dealerassociateid",
            "ismanual",
            col("senton").alias("timestamp"))\
    .dropna(subset=["timestamp"])

# Select only the BDC groups and produce a concise list of dealerassociateids, group name and virtual id
df_dag = df_dag.filter(col("name").isin("Service BDC", "Service Central"))\
               .select("id", "name", "virtualdealerassociateid")\
               .dropDuplicates()

# Join on BDC Group Members
df_dagmem = df_dagmem.select("dealerassociategroupid", "dealerassociateid")\
                     .join(df_dag, df_dagmem['dealerassociategroupid']==df_dag['id'], 'inner')\
                     .drop("id", "dealerassociategroupid")

# Produce a concise list of groups and virtual id names
df_dag = df_dag.select("name", "virtualdealerassociateid").dropDuplicates()

# Tag Virtual IDs in the Message Table
df_message = df_message.join(
                df_dag.select("virtualdealerassociateid").withColumn('isvirtual', lit(True)),
                df_message['dealerassociateid']==df_dag['virtualdealerassociateid'],
                'left')\
                .fillna(False)

# Tag which outbound (reps) belong to BDC (virtual) groups
df_message = df_message.join(
                df_dagmem.select("dealerassociateid").withColumn('isvirtualrep', lit(True)),
                'dealerassociateid',
                'left'
                )\
                .fillna(False)

# Creating Time Bins
df_message = df_message.withColumn('ut', unix_timestamp(col("timestamp"), 'yyyy-MM-dd HH:mm:ss'))\
                       .withColumn('dty', from_unixtime('ut'))\
                       .withColumn('date', date_format('dty', 'yyyy-MM-dd'))\
                       .withColumn('hour', date_format('dty', 'HH').alias('hour'))

# # Produce composite key column
df_message = df_message.withColumn("compositekey", concat_ws('-',df_message.dealerid, df_message.date, df_message.hour))

df_message.show()

"""
Creating Composite Key Table:
>>> This will be used as the common table for joins
>>> The current logic defines the composite key as the unique values between dealerid, date and hour
>>> Note: dealerassociateid is included in this table, but not the composite key since this would not provide good
linking between a BDC rep and an inbound. However we can still apply some filtering logic using dealerassociateid.
TODO: update this with a better definition.
"""
df_compkey = df_message.select("compositekey", "dealerid", "date", "hour").dropDuplicates()

"""
Separating Inbound and Response Logic
>>> This groups each valid inbound based on the criteria defined under the compositekey
"""
# Inbound Aggregations
df_inbounds = df_message.filter((col("isvirtual")==lit(True)) & (col("messagetype")=='I'))
df_inbounds_denorm = df_inbounds.groupBy("compositekey", "dealerassociateid").count()

# Response Logic
df_responses = df_message.filter((col("isvirtualrep")==lit(True)) & (col("messagetype")=='S'))

# Join the two dataframes
joined_df = df_inbounds.join(df_responses.select(
                                    "customerid",
                                    "date",
                                    col("dealerassociateid").alias("response_dealerassociateid"),
                                    col("ut").alias("response_ut"),
                                    col("timestamp").alias("response_timestamp")),
                                 ['customerid', 'date'],
                                  "left")

# Filter for responses which come after an inbound message
joined_df = joined_df.filter(col("response_ut")>col("ut"))

# Create a window specification
w = Window.partitionBy([col("customerid"), col("date")]).orderBy(col("response_ut"))

# Add a new column with the first value greater than the current row
result_df = joined_df.withColumn("first_greater", first(col("response_timestamp")).over(w)).dropDuplicates()

result_df.show()


#---
# dyf = DynamicFrame.fromDF(df, glueContext, 'dyf')
# # Use ResolveChoice to cast all columns as strings
# resolved_dyf = dyf.resolveChoice(choice = "cast:string")
# df_tmp = resolved_dyf.toDF()
# if not df_tmp.rdd.isEmpty():
#     df_tmp.show()
#     df_tmp.createOrReplaceTempView("incremental_input_data")
    
#     # # Register the input as temporary table to use in Iceberg Spark SQL statements
#     # df.createOrReplaceTempView("incremental_input_data")
#     # df.show()
#     df2 = spark.sql("""
#     SELECT * FROM incremental_input_data;
#     """)
#     df2.show()


# --

# df = glueContext.create_dynamic_frame.from_catalog(
#     database="boto3",
#     table_name="dealer_iceberg"
# )
