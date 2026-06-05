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
    from pyspark.sql.functions import col
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import StringType, ArrayType, IntegerType, LongType

    # 1) เอา DynamicFrame ตัวแรก แล้วแปลงเป็น Spark DF
    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()

    # 2) เปลี่ยนชื่อ link-href -> link_href (ถ้ามี)
    if "link-href" in df.columns and "link_href" not in df.columns:
        df = df.withColumnRenamed("link-href", "link_href")
    link_col = "link_href" if "link_href" in df.columns else ("link-href" if "link-href" in df.columns else None)

    # 3) ทำความสะอาดค่าใน target columns ให้รองรับทั้ง STRING/ARRAY<STRING>
    target_cols = [c for c in ["degree", "major", "skill", "certification"] if c in df.columns]
    nonempty_conds = []

    for c in target_cols:
        dt = df.schema[c].dataType
        if isinstance(dt, StringType):
            # "" หรือ "   " -> null
            df = df.withColumn(
                c,
                F.when(F.col(c).isNull(), F.lit(None))
                 .when(F.length(F.trim(F.col(c))) == 0, F.lit(None))
                 .otherwise(F.col(c))
            )
            nonempty_conds.append(F.col(c).isNotNull() & (F.length(F.trim(F.col(c))) > 0))

        elif isinstance(dt, ArrayType) and isinstance(dt.elementType, StringType):
            # กรองสมาชิกที่เป็น null/ว่าง/ช่องว่างออก
            cleaned = F.filter(F.col(c), lambda x: F.length(F.trim(x)) > 0)
            df = df.withColumn(c, cleaned)
            # non-empty array = มีขนาด > 0
            nonempty_conds.append(F.col(c).isNotNull() & (F.size(F.col(c)) > 0))

        else:
            # ชนิดอื่น ๆ: ถือว่า non-empty ถ้าไม่เป็น null
            nonempty_conds.append(F.col(c).isNotNull())

    # 4) เติมค่า position = forward-fill ต่อกลุ่ม + backfill ช่วงหัวกลุ่ม
    if link_col is not None and "position" in df.columns:
        # เก็บค่าเดิมเพื่อไม่ให้ถูกเขียนทับถ้าเดิมมีค่า
        df = df.withColumn("position_raw", F.col("position"))

        # ทำให้ค่าว่างจริงเป็น null
        df = df.withColumn(
            "position_clean",
            F.when(F.col("position").isNull(), F.lit(None))
             .when(F.length(F.trim(F.col("position"))) == 0, F.lit(None))
             .otherwise(F.col("position"))
        )

        # เลือกลำดับในกลุ่ม: ใช้ web-scraper-order ถ้ามี + tie-breaker เสมอ
        order_col = "web-scraper-order" if "web-scraper-order" in df.columns else None
        if order_col:
            if isinstance(df.schema[order_col].dataType, StringType):
                df = df.withColumn("_order_key", F.col(order_col).cast(LongType()))
            else:
                df = df.withColumn("_order_key", F.col(order_col).cast(LongType()))

        df = df.withColumn("_tie", F.monotonically_increasing_id())

        if order_col:
            w_rows = (
                Window.partitionBy(col(link_col))
                      .orderBy(F.col("_order_key").asc_nulls_last(), F.col("_tie"))
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )
        else:
            w_rows = (
                Window.partitionBy(col(link_col))
                      .orderBy(F.col("_tie"))
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )

        w_group = Window.partitionBy(col(link_col))
        
        # ค่าก่อนหน้า (รวมปัจจุบัน) ที่ไม่ null ในกลุ่ม จาก position_clean
        ffill = F.last(F.col("position_clean"), ignorenulls=True).over(w_rows)
        # ค่าที่ไม่ว่างตัวแรกของทั้งกลุ่ม
        first_in_group = F.first(F.col("position_clean"), ignorenulls=True).over(w_group)
        # ค่าดั้งเดิมถ้าไม่ว่าง (ห้ามถูกเขียนทับ)
        orig_nonempty = F.when(F.length(F.trim(F.col("position_raw"))) > 0, F.col("position_raw"))

        # อย่าเขียนทับถ้าเดิมมีค่า → เดิม > ffill > first_in_group
        df = (
            df.withColumn("position", F.coalesce(orig_nonempty, ffill, first_in_group))
              .drop("_tie", "_order_key")
              .drop("position_raw", "position_clean")
        )
    # 5) กรอง: ลบแถวที่ target_cols "ว่างทั้งหมด" (คงไว้ถ้ามีอย่างน้อย 1 คอลัมน์ไม่ว่าง)
    if nonempty_conds:
        keep_any = nonempty_conds[0]
        for cond in nonempty_conds[1:]:
            keep_any = keep_any | cond
        df = df.filter(keep_any)

    # 6) คืนเป็น DynamicFrame
    dyf_out = DynamicFrame.fromDF(df, glueContext, "Cleaned1")
    return DynamicFrameCollection({"Cleaned1": dyf_out}, glueContext)
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
AmazonS3_node1756871684882 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jobfit-g55/transformed/jobgov/set1/set2.4_manageGov05092025/"], "recurse": True}, transformation_ctx="AmazonS3_node1756871684882")

# Script generated for node Custom Transform
CustomTransform_node1756871962562 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1756871684882": AmazonS3_node1756871684882}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1756875856121 = SelectFromCollection.apply(dfc=CustomTransform_node1756871962562, key=list(CustomTransform_node1756871962562.keys())[0], transformation_ctx="SelectFromCollection_node1756875856121")

# Script generated for node Drop Fields
DropFields_node1756875813482 = DropFields.apply(frame=SelectFromCollection_node1756875856121, paths=["jobqualification1", "place_date", "linkapply-herf", "announce-herf", "date", "web-scraper-order", "link_href"], transformation_ctx="DropFields_node1756875813482")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1756875813482, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1756872833008", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1756875876519 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1756875813482, connection_type="s3", format="json", connection_options={"path": "s3://jobfit-g55/transformed/jobgov/set1/set3.3_manageGov04092025/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1756875876519")

job.commit()