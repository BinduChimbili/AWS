import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1781869137522 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://binduprojectdata/stagedata/employee_data_2026-07-16_15-33-08.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1781869137522")

# Script generated for node SQL Query
SqlQuery1191 = '''
select * from myDataSource;
'''
SQLQuery_node1781869142727 = sparkSqlQuery(glueContext, query = SqlQuery1191, mapping = {"myDataSource":AmazonS3_node1781869137522}, transformation_ctx = "SQLQuery_node1781869142727")

# Script generated for node Amazon Redshift
AmazonRedshift_node1781869146137 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1781869142727, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-604371741748-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "\"public\".\"project_new\"", "connectionName": "batch7-testrs-connection-new", "preactions": "CREATE TABLE IF NOT EXISTS \"public\".\"project_new\" (\"id\" VARCHAR, \"name\" VARCHAR, \"age\" VARCHAR, \"gender\" VARCHAR, \"department\" VARCHAR, \"position\" VARCHAR, \"salary\" VARCHAR, \"joining_date\" VARCHAR, \"experience_years\" VARCHAR, \"last_modified_timestamp\" VARCHAR, \"historical_flag\" VARCHAR, \"incremental_flag\" VARCHAR);"}, transformation_ctx="AmazonRedshift_node1781869146137")

job.commit()