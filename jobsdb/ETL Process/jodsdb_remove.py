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
    from pyspark.sql.functions import (
        col, trim, regexp_replace, regexp_extract, split, when, lower
    )
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from functools import reduce

    transformed_frames = {}

    # เตรียม regex จังหวัด
    provinces = [
        "พิษณุโลก","สุโขทัย","เพชรบูรณ์","พิจิตร","กำแพงเพชร","นครสวรรค์","ลพบุรี","ชัยนาท","อุทัยธานี",
        "สิงห์บุรี","อ่างทอง","สระบุรี","พระนครศรีอยุธยา","สุพรรณบุรี","นครนายก","ปทุมธานี","นนทบุรี",
        "นครปฐม","สมุทรปราการ","สมุทรสาคร","สมุทรสงคราม","หนองคาย","บึงกาฬ","นครพนม","สกลนคร","อุดรธานี",
        "หนองบัวลำภู","เลย","มุกดาหาร","กาฬสินธุ์","ขอนแก่น","อำนาจเจริญ","ยโสธร","ร้อยเอ็ด","มหาสารคาม",
        "ชัยภูมิ","นครราชสีมา","บุรีรัมย์","สุรินทร์","ศรีสะเกษ","อุบลราชธานี","ชุมพร","ระนอง","สุราษฎร์ธานี",
        "นครศรีธรรมราช","กรุงเทพมหานคร","กระบี่","พังงา","ภูเก็ต","พัทลุง","ตรัง","ปัตตานี","สงขลา","สตูล",
        "นราธิวาส","ยะลา","เชียงใหม่","แม่ฮ่องสอน","เชียงราย","ลำพูน","ลำปาง","พะเยา","แพร่","น่าน","อุตรดิตถ์",
        "ตาก","กาญจนบุรี","ราชบุรี","เพชรบุรี","ประจวบคีรีขันธ์","สระแก้ว","ปราจีนบุรี","ฉะเชิงเทรา","ชลบุรี",
        "ระยอง","จันทบุรี","ตราด"
    ]
    province_pattern = "(" + "|".join(provinces) + "|กรุงเทพฯ)"

    for key in dfc.keys():
        dyf = dfc.select(key)
        df = dyf.toDF()

        # -------- position_clean --------
        base_position = trim(
            regexp_extract(
                split(col("link"), ",").getItem(0),
                r"^(.*?)(\(|$)", 1
            )
        )
        base_position = regexp_replace(base_position, r"^【.*?】[:：\-\s]*", "")
        base_position = regexp_replace(base_position, r"\s*-\s*(JLPT|ประจำ|location|Bangkok|up to|สัมภาษณ์).*?$", "")
        base_position = regexp_replace(base_position, r"(?i)\bnew graduate are welcome\b", "")
        base_position = regexp_replace(base_position, r"(?i)\bประจำ[^\s]*", "")
        base_position = regexp_replace(base_position, r"(?i)(\bup to\b|฿\d+[\d,]*|\d{1,3}[,\d]* ?k)", "")
        base_position = regexp_replace(base_position, r"(?i)สัมภาษณ์.*?(ม\.ค\.|ก\.พ\.|มี\.ค\.|เม\.ย\.|พ\.ค\.|มิ\.ย\.|ก\.ค\.|ส\.ค\.|ก\.ย\.|ต\.ค\.|พ\.ย\.|ธ\.ค\.|\d{1,2}\s*[ก-ฮ]\.?\s*\d{2,4})?", "")
        base_position = regexp_replace(base_position, r"\(ID:?\d+\)", "")
        base_position = regexp_replace(base_position, r"\([^)]*business[^)]*\)", "")
        base_position = regexp_replace(base_position, r"\([^)]*\d{4,}\)", "")
        base_position = regexp_replace(base_position, r"[-–\s]+$", "")
        base_position = regexp_replace(base_position, r"\d+", "")
        df = df.withColumn("position", trim(base_position))

        # -------- announce --------
        df = df.withColumn("announce", col("link-href"))

        # -------- company_clean --------
        df = df.withColumn(
            "company_clean",
            trim(regexp_replace(col("company"), r"ดู(งาน)?ทั้งหมด", ""))
        )

        # -------- province_clean --------
        df = df.withColumn(
            "province_clean",
            regexp_extract(col("province"), province_pattern, 1)
        )
        df = df.withColumn(
            "province_clean",
            when(col("province_clean") == "กรุงเทพฯ", "กรุงเทพมหานคร")
            .otherwise(col("province_clean"))
        )

        # -------- salary_clean --------
        df = df.withColumn(
            "salary_clean",
            when(
                (col("salary").isNull()) | (trim(col("salary")) == ""),
                "Not specified"
            ).otherwise(
                regexp_replace(trim(col("salary")), r"[\n\r\t]+", " ")
            )
        )
        df = df.withColumn(
            "salary_clean",
            regexp_replace(col("salary_clean"), r"\bp\.m\.\b", "per month")
        )

        cleaned_dyf = DynamicFrame.fromDF(df, glueContext, f"cleaned_{key}")
        transformed_frames[key] = cleaned_dyf

    return DynamicFrameCollection(transformed_frames, glueContext)
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
AmazonS3_node1758208603362 = glueContext.create_dynamic_frame.from_catalog(database="jobscrape", table_name="scraping_jobsdb", transformation_ctx="AmazonS3_node1758208603362")

# Script generated for node Custom Transform
CustomTransform_node1758208683832 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1758208603362": AmazonS3_node1758208603362}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1758209031632 = SelectFromCollection.apply(dfc=CustomTransform_node1758208683832, key=list(CustomTransform_node1758208683832.keys())[0], transformation_ctx="SelectFromCollection_node1758209031632")

# Script generated for node Drop Fields
DropFields_node1758209041641 = DropFields.apply(frame=SelectFromCollection_node1758209031632, paths=["web-scraper-order", "web-scraper-start-url", "link", "link-href", "pagination", "salary", "salary_clean", "company", "province", "announce"], transformation_ctx="DropFields_node1758209041641")

# Script generated for node Rename Field
RenameField_node1758209277755 = RenameField.apply(frame=DropFields_node1758209041641, old_name="province_clean", new_name="province", transformation_ctx="RenameField_node1758209277755")

# Script generated for node Rename Field
RenameField_node1758209277861 = RenameField.apply(frame=RenameField_node1758209277755, old_name="position", new_name="position", transformation_ctx="RenameField_node1758209277861")

# Script generated for node Rename Field
RenameField_node1758209371106 = RenameField.apply(frame=RenameField_node1758209277861, old_name="company_clean", new_name="organization", transformation_ctx="RenameField_node1758209371106")

# Script generated for node Rename Field
RenameField_node1758209371240 = RenameField.apply(frame=RenameField_node1758209371106, old_name="employment", new_name="type", transformation_ctx="RenameField_node1758209371240")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RenameField_node1758209371240, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758208561765", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758209820160 = glueContext.write_dynamic_frame.from_options(frame=RenameField_node1758209371240, connection_type="s3", format="json", connection_options={"path": "s3://jobfit-g55-1/transformed/jobsdb/jodsdb_not_skill/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1758209820160")

job.commit()