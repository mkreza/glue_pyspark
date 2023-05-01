import pytest
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys
sys.path.insert(1, 'src')
import sample

from pyspark.conf import SparkConf


@pytest.fixture(scope="module", autouse=True)
def glue_context():
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

    sc = SparkContext(conf=conf)
    glueContext = GlueContext(sc)
    # spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    yield(glueContext)

    job.commit()


def test_counts(glue_context):
    dyf = sample.read_json(glue_context, "s3://awsglue-datasets/examples/us-legislators/all/persons.json")
    df = dyf.toDF()
    df.createOrReplaceTempView("incremental_input_data")
    # df.show()
    spark = glue_context.spark_session

    df2 = spark.sql("""
    SELECT * FROM incremental_input_data
    """)
    df2.show()
    assert df2.count() == 1961