import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, size

    # แปลง DynamicFrame เป็น DataFrame
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # ลบแถวที่ skill เป็น null หรือ array ว่าง
    df_clean = df.filter((col("skill").isNotNull()) & (size(col("skill")) > 0))

    # แปลงกลับเป็น DynamicFrame
    dyf_clean = DynamicFrame.fromDF(df_clean, glueContext, "dyf_clean")

    return DynamicFrameCollection({"CustomTransform0": dyf_clean}, glueContext)
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

# Script generated for node Amazon S3
AmazonS3_node1758214985627 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jobfit-g55-1/transformed/jobsdb/jobsdb_all4/"], "recurse": True}, transformation_ctx="AmazonS3_node1758214985627")

# Script generated for node Custom Transform
CustomTransform_node1758215024817 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1758214985627": AmazonS3_node1758214985627}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1758215234136 = SelectFromCollection.apply(dfc=CustomTransform_node1758215024817, key=list(CustomTransform_node1758215024817.keys())[0], transformation_ctx="SelectFromCollection_node1758215234136")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1758215234136, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758214955577", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758215241209 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1758215234136, connection_type="s3", format="json", connection_options={"path": "s3://jobfit-g55-1/transformed/jobsdb/jobsdb_fish/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1758215241209")

job.commit()