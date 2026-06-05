import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# Script generated for node คลีน announce, position_clean, province_clean, employment_clean
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # ===== Imports (ใส่ในฟังก์ชันเพื่อเลี่ยง NameError บน Glue) =====
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import (
        col, split, trim, explode, lower, collect_set, length, when, regexp_replace,
        udf, lit
    )
    from pyspark.sql.types import StringType
    import re

    # ===== Config: ACRONYMS ที่ต้องคงเป็น UPPER =====
    ACRONYMS = {
        "AI","BI","BD","CRM","SCM","PMT","APAC","F&B","IT","HR","GTU","R&D",
        "QA","QC","KOL","BKK","SEA","SCG","CP","OD","UX","UI"
    }

    # ===== UDF: ทำความสะอาด position และ normalize caps =====
    def clean_position(text):
        if not text:
            return None
        s = str(text)

        # 1) ลบ "with verification" (case-insensitive)
        s = re.sub(r"\bwith\s+verification\b", "", s, flags=re.IGNORECASE)

        # 2) unify dash
        s = s.replace("–", "-").replace("—", "-")

        # 3) จัดช่องว่าง/บรรทัด
        s = s.replace("\n", " ").replace("\r", " ")
        s = s.strip(' "\'')
        s = re.sub(r"\s+", " ", s).strip()

        # 4) ถ้าข้อความเป็นรูปแบบ T + T (ซ้ำทั้งประโยค) ให้เหลือครั้งเดียว (ทำซ้ำจนไม่แมตช์)
        pattern = re.compile(r"^(?P<x>.+?)\s*\1$")
        last = None
        while s and s != last:
            last = s
            m = pattern.match(s)
            if m:
                s = m.group("x").strip()
            else:
                break

        # 5) normalize ตัวพิมพ์ โดยคง ACRONYMS ให้เป็น UPPER
        def norm_token(tok):
            t = tok.strip()
            if not t:
                return t
            alnum = re.sub(r"[^A-Za-z0-9&+/]", "", t).upper()
            if alnum in ACRONYMS:
                return alnum  # คง/บังคับเป็น UPPER
            # title-case เบา ๆ สำหรับคำอื่น
            return t[:1].upper() + t[1:]
        # แยกด้วยกลุ่ม non-word เพื่อคงตัวคั่นเดิม
        parts = re.split(r"(\W+)", s)
        parts = [norm_token(p) if i % 2 == 0 else p for i, p in enumerate(parts)]
        s = "".join(parts).strip()

        return s or None

    clean_position_udf = udf(clean_position, StringType())

    # ===== UDF: ดึงจังหวัด =====
    THAI_PROVINCES = [
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
    SPECIAL_MAP = {
        "เขตปริมณฑลกรุงเทพมหานคร": "กรุงเทพมหานคร",
        "กรุงเทพฯ": "กรุงเทพมหานคร",
        "อำเภอเมืองนนทบุรี": "นนทบุรี",
        "เมืองนนทบุรี": "นนทบุรี",
        "อำเภอเมืองระยอง": "ระยอง",
        "เมืองระยอง": "ระยอง",
    }

    def extract_province(text):
        if not text:
            return "Not specified"
        s = str(text).replace("\n", " ").replace("\r", " ")
        s = re.sub(r"\s+", " ", s)
        for k, v in SPECIAL_MAP.items():
            if k in s:
                return v
        for prov in THAI_PROVINCES:
            if prov in s:
                return prov
        return "Not specified"

    extract_province_udf = udf(extract_province, StringType())

    # ===== UDF: ดึงประเภทจ้างงาน =====
    EMP_TYPE = ["พนักงานประจำ", "งานพาร์ทไทม์", "งานสัญญาจ้าง", "งานชั่วคราว", "นักศึกษาฝึกงาน"]

    def extract_emp_type(text):
        if not text:
            return "Not specified"
        s = str(text).replace("\n", " ").replace("\r", " ")
        s = re.sub(r"\s+", " ", s).strip()
        for t in EMP_TYPE:
            if t in s:
                return t
        return "Not specified"

    extract_emp_type_udf = udf(extract_emp_type, StringType())

    # ===== Helper: เช็คค่าว่างหลัง trim =====
    def is_nonempty(cname):
        return (col(cname).isNotNull()) & (length(trim(col(cname))) > 0)

    # ===== Main loop =====
    out = {}

    for key in dfc.keys():
        dyf = dfc.select(key)
        df = dyf.toDF()

        # --- สร้าง announce จาก `position-href` (ถ้ามี) ---
        # ถ้าไม่มี ให้ใส่ null เอาไว้ แล้วให้กฎ validation เป็นคนคัดทิ้ง
        if "position-href" in df.columns:
            df = df.withColumn("announce", col("position-href").cast("string"))
        else:
            df = df.withColumn("announce", lit(None).cast("string"))

        # --- ทำความสะอาด position ---
        base_pos_col = "position" if "position" in df.columns else None
        if base_pos_col:
            df = df.withColumn("position_clean", clean_position_udf(col(base_pos_col)))
        else:
            df = df.withColumn("position_clean", lit(None).cast("string"))

        # --- Province / Employment ---
        if "province" in df.columns:
            df = df.withColumn("province_clean", extract_province_udf(col("province")))
        else:
            df = df.withColumn("province_clean", lit("Not specified").cast("string"))

        if "employment" in df.columns:
            df = df.withColumn("employment_clean", extract_emp_type_udf(col("employment")))
        else:
            df = df.withColumn("employment_clean", lit("Not specified").cast("string"))

        # --- Validation flags ---
        has_position = is_nonempty("position_clean")
        has_announce = is_nonempty("announce") & col("announce").rlike(r"^https?://")
        has_id = is_nonempty("id") if "id" in df.columns else lit(True)

        df = df.withColumn("is_valid", has_position & has_announce & has_id)

        # --- ตัดเคสฝากเรซูเม่ (หากมีคอลัมน์และข้อความนี้) ---
        resume_text = "ยังไม่พบตำแหน่งที่ต้องการ (ต้องการฝาก Resume)"
        df = df.withColumn(
            "is_resume_drop",
            when(trim(col("position_clean")) == lit(resume_text), lit(True)).otherwise(lit(False))
        )

        # --- แยก clean / rejected ---
        df_clean = df.filter(col("is_valid") & (~col("is_resume_drop"))).drop("is_valid", "is_resume_drop")
        df_rejected = df.filter(~(col("is_valid") & (~col("is_resume_drop"))))

        # --- เข้าสู่ DynamicFrame ---
        out[f"{key}_clean"] = DynamicFrame.fromDF(df_clean, glueContext, f"{key}_clean")
        out[f"{key}_rejected"] = DynamicFrame.fromDF(df_rejected, glueContext, f"{key}_rejected")

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

# Script generated for node linkedin
linkedin_node1758197371222 = glueContext.create_dynamic_frame.from_catalog(database="jobscrape", table_name="scraping_linkedin", transformation_ctx="linkedin_node1758197371222")

# Script generated for node คลีน announce, position_clean, province_clean, employment_clean
announceposition_cleanprovince_cleanemployment_clean_node1758197478674 = MyTransform(glueContext, DynamicFrameCollection({"linkedin_node1758197371222": linkedin_node1758197371222}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1758197998125 = SelectFromCollection.apply(dfc=announceposition_cleanprovince_cleanemployment_clean_node1758197478674, key=list(announceposition_cleanprovince_cleanemployment_clean_node1758197478674.keys())[0], transformation_ctx="SelectFromCollection_node1758197998125")

# Script generated for node Drop web-scraper-order
Dropwebscraperorder_node1758199783169 = DropFields.apply(frame=SelectFromCollection_node1758197998125, paths=["web-scraper-order"], transformation_ctx="Dropwebscraperorder_node1758199783169")

# Script generated for node web-scraper-start-url
webscraperstarturl_node1758200221602 = DropFields.apply(frame=Dropwebscraperorder_node1758199783169, paths=["web-scraper-start-url"], transformation_ctx="webscraperstarturl_node1758200221602")

# Script generated for node position
position_node1758200249716 = DropFields.apply(frame=webscraperstarturl_node1758200221602, paths=["position"], transformation_ctx="position_node1758200249716")

# Script generated for node position-href
positionhref_node1758200265487 = DropFields.apply(frame=position_node1758200249716, paths=["position-href"], transformation_ctx="positionhref_node1758200265487")

# Script generated for node province
province_node1758200350467 = DropFields.apply(frame=positionhref_node1758200265487, paths=["province"], transformation_ctx="province_node1758200350467")

# Script generated for node employment
employment_node1758200369963 = DropFields.apply(frame=province_node1758200350467, paths=["employment"], transformation_ctx="employment_node1758200369963")

# Script generated for node pagination
pagination_node1758200370053 = DropFields.apply(frame=employment_node1758200369963, paths=["pagination"], transformation_ctx="pagination_node1758200370053")

# Script generated for node Rename province
Renameprovince_node1758200416472 = RenameField.apply(frame=pagination_node1758200370053, old_name="province_clean", new_name="province", transformation_ctx="Renameprovince_node1758200416472")

# Script generated for node Rename position
Renameposition_node1758200451582 = RenameField.apply(frame=Renameprovince_node1758200416472, old_name="position_clean", new_name="position", transformation_ctx="Renameposition_node1758200451582")

# Script generated for node Rename organization
Renameorganization_node1758200451714 = RenameField.apply(frame=Renameposition_node1758200451582, old_name="company", new_name="organization", transformation_ctx="Renameorganization_node1758200451714")

# Script generated for node Rename Field
RenameField_node1758200865963 = RenameField.apply(frame=Renameorganization_node1758200451714, old_name="employment_clean", new_name="type", transformation_ctx="RenameField_node1758200865963")

# Script generated for node linkedin-not skill
EvaluateDataQuality().process_rows(frame=RenameField_node1758200865963, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758200835204", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
linkedinnotskill_node1758201006708 = glueContext.write_dynamic_frame.from_options(frame=RenameField_node1758200865963, connection_type="s3", format="json", connection_options={"path": "s3://jobfit-g55-1/transformed/linkedin/linkedin_not_skill/", "partitionKeys": []}, transformation_ctx="linkedinnotskill_node1758201006708")

job.commit()