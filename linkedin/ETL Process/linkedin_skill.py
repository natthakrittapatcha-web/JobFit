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
    from pyspark.sql.functions import (
        col, split, trim, regexp_replace, array_distinct, coalesce, lit,
        transform, filter as ffilter
    )

    out = {}

    for key in dfc.keys():
        dyf = dfc.select(key)
        df = dyf.toDF()

        # --- เพิ่มคอลัมน์ skills_clean จาก skills_text ---
        if "skills_text" in df.columns:
            df = df.withColumn(
                "skills_clean",
                array_distinct(
                    ffilter(
                        transform(
                            split(coalesce(col("skills_text"), lit("")), "เพิ่ม"),
                            lambda x: trim(regexp_replace(x, r"\s+", " "))
                        ),
                        lambda x: x != ""
                    )
                )
            )
        else:
            df = df.withColumn("skills_clean", lit(None).cast("array<string>"))

        # --- รวมทุก partition เป็น 1 partition เพื่อให้ได้ไฟล์เดียวเวลาเขียนลง S3 ---
        df = df.coalesce(1)

        # แปลงกลับเป็น DynamicFrame พร้อมทุกคอลัมน์เดิม + skills_clean
        out[key] = DynamicFrame.fromDF(df, glueContext, key)

    return DynamicFrameCollection(out, glueContext)
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
AmazonS3_node1758201519751 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jobfit-g55-1/transformed/linkedin/linkedin_not_skill/run-1758201280556-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1758201519751")

# Script generated for node Custom Transform
CustomTransform_node1758201674632 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1758201519751": AmazonS3_node1758201519751}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1758202083503 = SelectFromCollection.apply(dfc=CustomTransform_node1758201674632, key=list(CustomTransform_node1758201674632.keys())[0], transformation_ctx="SelectFromCollection_node1758202083503")

# Script generated for node Drop Fields
DropFields_node1758202098446 = DropFields.apply(frame=SelectFromCollection_node1758202083503, paths=["skills_text"], transformation_ctx="DropFields_node1758202098446")

# Script generated for node Rename skill
Renameskill_node1758202112465 = RenameField.apply(frame=DropFields_node1758202098446, old_name="skills_clean", new_name="skill", transformation_ctx="Renameskill_node1758202112465")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Renameskill_node1758202112465, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758200835204", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758202433230 = glueContext.write_dynamic_frame.from_options(frame=Renameskill_node1758202112465, connection_type="s3", format="json", connection_options={"path": "s3://jobfit-g55-1/transformed/linkedin/linkedin_yes_skill/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1758202433230")

job.commit()