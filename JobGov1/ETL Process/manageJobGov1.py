import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Delete row N column
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, regexp_extract, when, length, lit
    from awsglue.dynamicframe import DynamicFrame
    import re

    # ดึงข้อมูลจาก DynamicFrame
    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()

    # ลบคอลัมน์ linkApply และ Announcement
    df = df.drop("linkApply", "Announcement")

    # กรองแถวที่ข้อมูลในคอลัมน์ JobQualification1 ซ้ำกับ place_date
    df = df.filter(col("JobQualification1") != col("place_date"))

    df = df.filter(
        ~col("JobQualification1").rlike(
            r"^[\p{Pd}\#\_\+\=\!@\$%\^&\*\(\)\[\]\{\}\|\\\/:;\"'`~,.<>?]+$"
        )
    )


    # คำที่ไม่ต้องการใน JobQualification1
    unwanted_keywords_start = [
        "หมายเหตุ", "ที่มา", "ติดต่อ", "การสมัคร", "วิธีการสมัคร", "กำหนดวันรับสมัคร", "ประกาศรับสมัคร", 
        "ตั้งแต่วันที่", "ผู้มีสิทธิสมัคร", "ผู้สนใจ", "ผู้ที่มีความประสงค์", "ผู้มีความประสงค์", "วัน", 
        "รายละเอียด", "ลิงค์สมัคร", "ผู้ประสงค์","สถานที่รับสมัคร", "กำหนดการ", "รับสมัครทาง",
        "รับสมัครตั้งแต่", "ทั้งนี้", "สมัครงาน", "เปิดรับสมัคร" ,"ให้ผู้ประสงค์", "กำหนดการรับสมัคร",
        "ผู้ที่สนใจ", "ปรกาศ", "หลักฐาน", "สถานที่", "รับสมัครระหว่าง", "ประกาศรับ", "สมัคร"
        ]

    unwanted_keywords_any = [
        "ยื่นผลคะแนน", "ยื่นใบสมัคร", "ใบสมัคร", "ดาวน์โหลด", "ระยะเวลารับสมัคร", "ตรวจสอบได้ที่", "เปิดรับสมัคร",  "ยื่นใบ",
        "ผ่านเว็บไซต์", "หมู่ที่", "ประสงค์", "มีความประสงค์"
        ]
        
    # สร้าง regex pattern สำหรับคำที่ไม่ต้องการให้เป็นคำแรก
    unwanted_pattern_start = "^(" + "|".join([re.escape(keyword) for keyword in unwanted_keywords_start]) + ")"

    # สร้าง regex pattern สำหรับคำที่ไม่ต้องการอยู่ใน JobQualification1
    unwanted_pattern_any = "|".join([re.escape(keyword) for keyword in unwanted_keywords_any])

    # กรองแถวที่ JobQualification1 เริ่มต้นด้วยคำที่ไม่ต้องการ
    df = df.filter(~col("JobQualification1").rlike(unwanted_pattern_start))

    # กรองแถวที่ JobQualification1 มีคำที่ไม่ต้องการ (ไม่จำเป็นต้องเป็นคำแรก)
    df = df.filter(~col("JobQualification1").rlike(unwanted_pattern_any))

    # แปลงกลับเป็น DynamicFrame
    dyf_cleaned = DynamicFrame.fromDF(df, glueContext, "dyf_cleaned")

    # ส่งคืนข้อมูล
    return DynamicFrameCollection({"Cleaned": dyf_cleaned}, glueContext)
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

# Script generated for node DataSimple
DataSimple_node1754671430201 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jobfit-g55/transformed/jobgov/set1/runSimple_08Aug2025_1754671275587/"], "recurse": True}, transformation_ctx="DataSimple_node1754671430201")

# Script generated for node Drop Fields
DropFields_node1754671627911 = DropFields.apply(frame=DataSimple_node1754671430201, paths=["announcement", "web-scraper-start-url", "link", "pagination", "linkapply", "tag"], transformation_ctx="DropFields_node1754671627911")

# Script generated for node Delete row N column
DeleterowNcolumn_node1754671591613 = MyTransform(glueContext, DynamicFrameCollection({"DropFields_node1754671627911": DropFields_node1754671627911}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1754675443327 = SelectFromCollection.apply(dfc=DeleterowNcolumn_node1754671591613, key=list(DeleterowNcolumn_node1754671591613.keys())[0], transformation_ctx="SelectFromCollection_node1754675443327")

# Script generated for node Rename apply
Renameapply_node1754797516630 = RenameField.apply(frame=SelectFromCollection_node1754675443327, old_name="linkApply-href", new_name="linkapply-herf", transformation_ctx="Renameapply_node1754797516630")

# Script generated for node Rename announce
Renameannounce_node1754797520021 = RenameField.apply(frame=Renameapply_node1754797516630, old_name="JobQualification1", new_name="jobqualification1", transformation_ctx="Renameannounce_node1754797520021")

# Script generated for node Rename qualification
Renamequalification_node1754676201435 = RenameField.apply(frame=Renameannounce_node1754797520021, old_name="Announcement-href", new_name="announce-herf", transformation_ctx="Renamequalification_node1754676201435")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Renamequalification_node1754676201435, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754797453916", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1754801754726 = glueContext.write_dynamic_frame.from_options(frame=Renamequalification_node1754676201435, connection_type="s3", format="json", connection_options={"path": "s3://jobfit-g55/transformed/jobgov/set1/set1_manageGov11082025/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1754801754726")

job.commit()