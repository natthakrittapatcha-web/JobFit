import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql("(select * from source1) UNION " + unionType + " (select * from source2)")
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    # แปลง DynamicFrame เป็น DataFrame
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # รวม partition เหลือไฟล์เดียว
    df_single = df.coalesce(1)

    # แปลงกลับเป็น DynamicFrame
    dyn = DynamicFrame.fromDF(df_single, glueContext, "df_single")

    return DynamicFrameCollection({"CustomTransform0": dyn}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node linkedin_jobsdb
linkedin_jobsdb_node1758278329491 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jobfit-g55-1/load/run-1758251514683-part-r-00000"], "recurse": True}, transformation_ctx="linkedin_jobsdb_node1758278329491")

# Script generated for node gov
gov_node1758278432829 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jobfit-g55-1/Gov1_sum/run-1758277677995-part-r-00000"], "recurse": True}, transformation_ctx="gov_node1758278432829")

# Script generated for node SQL Query
SqlQuery121 = '''
SELECT
  province,
  position,
  organization,
  degree,
  major,
  skill,
  split(certification, ',') AS certification,  -- แปลง string → array<string>
  type
FROM linkedin_jobsdb;
'''
SQLQuery_node1758294298184 = sparkSqlQuery(glueContext, query = SqlQuery121, mapping = {"linkedin_jobsdb":linkedin_jobsdb_node1758278329491}, transformation_ctx = "SQLQuery_node1758294298184")

# Script generated for node Union
Union_node1758294945080 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": SQLQuery_node1758294298184, "source2": gov_node1758278432829}, transformation_ctx = "Union_node1758294945080")

# Script generated for node Custom Transform
CustomTransform_node1758293674079 = MyTransform(glueContext, DynamicFrameCollection({"Union_node1758294945080": Union_node1758294945080}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1758295023371 = SelectFromCollection.apply(dfc=CustomTransform_node1758293674079, key=list(CustomTransform_node1758293674079.keys())[0], transformation_ctx="SelectFromCollection_node1758295023371")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1758295023371, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758293418175", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758295028937 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1758295023371, connection_type="s3", format="json", connection_options={"path": "s3://jobfit-g55-1/fish/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1758295028937")

job.commit()