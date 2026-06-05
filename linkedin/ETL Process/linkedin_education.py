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
    from pyspark.sql.functions import col, udf, lit
    from pyspark.sql.types import ArrayType, StringType
    import pyspark.sql.functions as F
    import re

    # -------------------------
    # 1) ตั้งค่า Keyword / Regex
    # -------------------------
    DEGREE_PATTERNS = [
        r"bachelor[’']?s\s+degree",
        r"master[’']?s\s+degree",
        r"ph\.?d|doctorate",
        r"associate[’']?s\s+degree",
        r"high\s*school\s*diploma",
        r"diploma",
        r"ged\b",
        r"bachelor[’']?s\s+degree\s+in\s+any\s+(field|discipline|major|background)",
        r"any\s+bachelor[’']?s\s+degree",
        r"วุฒิการศึกษา",
        r"ปริญญา(?:ตรี|โท|เอก)",
        r"ป\.?ตรี", r"ป\.?โท", r"ป\.?เอก",
        r"(?:จบการศึกษา)?(?:ระดับ)?\s*ปริญญา(?:ตรี|โท|เอก)?",
        r"อนุปริญญา",
        r"ประกาศนียบัตร",
        r"มัธยมศึกษาตอนปลาย|ม\.?\s*6",
        r"มัธยมศึกษาตอนต้น|ม\.?\s*3",
        r"ปวช\.?",
        r"ปวส\.?"
    ]

    MAJOR_TERMS = [
        # ตัวอย่างสั้นลงเพื่อความกระชับ
        "computer science", "information technology", "data science", "วิทยาการคอมพิวเตอร์", "เทคโนโลยีสารสนเทศ",
        "marketing", "business", "finance", "บัญชี", "เศรษฐศาสตร์",
        "medicine", "แพทยศาสตร์", "nursing", "พยาบาลศาสตร์"
    ]

    CERT_PATTERNS = [
        r"toeic\s*\d{2,4}",
        r"toefl\s*(ibt|pbt|cbt)?\s*\d{2,4}",
        r"ielts\s*[\d\.]+",
        r"toeic|toefl|ielts",
        r"ใบรับรอง|ประกาศนียบัตร|ใบอนุญาต"
    ]

    # -------------------------
    # 2) Compile Regex
    # -------------------------
    DEGREE_RX = re.compile(r"(?:" + r"|".join(DEGREE_PATTERNS) + r")", re.IGNORECASE)
    CERT_RX   = re.compile(r"(?:" + r"|".join(CERT_PATTERNS) + r")", re.IGNORECASE)
    MAJOR_RX  = re.compile("|".join([re.escape(t) for t in MAJOR_TERMS]), re.IGNORECASE)
    SENT_SPLIT_RX = re.compile(r"(?:\n+|[•\u2022·\u2023\u25E6\u2043\-*]+|\s{2,}|(?<=[\.!?])\s+)")

    # -------------------------
    # 3) Helper functions
    # -------------------------
    def split_sentences(text):
        if not text: return []
        s = str(text)
        parts = [p.strip(" \t\r\n-•\u2022·") for p in SENT_SPLIT_RX.split(s)]
        return [p for p in parts if p]

    def uniq_keep_order(values):
        seen = set(); out = []
        for v in values or []:
            k = v.strip().lower()
            if k and k not in seen:
                seen.add(k); out.append(v.strip())
        return out

    def _extract(rx, arr):
        out = []
        for sent in arr or []:
            out += [m.group(0) for m in rx.finditer(sent)]
        return uniq_keep_order(out)

    def extract_degree_udf_fn(text): return _extract(DEGREE_RX, split_sentences(text))
    def extract_major_udf_fn(text):  return _extract(MAJOR_RX, split_sentences(text))
    def extract_cert_udf_fn(text):
        arr = _extract(CERT_RX, split_sentences(text))
        return arr[0] if arr else None

    def normalize_degree_from_major_udf_fn(text):
        deg = extract_degree_udf_fn(text)
        maj = extract_major_udf_fn(text)
        if not deg and maj:
            return ["Bachelor’s Degree"]
        return deg or []

    # -------------------------
    # 4) Register UDFs
    # -------------------------
    extract_degree_udf  = udf(extract_degree_udf_fn, ArrayType(StringType()))
    extract_major_udf   = udf(extract_major_udf_fn, ArrayType(StringType()))
    extract_cert_udf    = udf(extract_cert_udf_fn, StringType())
    norm_degree_udf     = udf(normalize_degree_from_major_udf_fn, ArrayType(StringType()))

    # -------------------------
    # 5) Apply to each DynamicFrame
    # -------------------------
    out = {}
    for key in dfc.keys():
        dyf = dfc.select(key)
        df  = dyf.toDF()

        if 'text' not in df.columns:
            df = df.withColumn('text', lit(None).cast(StringType()))
        else:
            df = df.withColumn(
                'text',
                F.when(F.length(F.trim(F.col('text'))) == 0, lit(None).cast(StringType()))
                 .otherwise(F.col('text').cast(StringType()))
            )

        # Extract
        df = df.withColumn("certificate", extract_cert_udf(F.col("text"))) \
               .withColumn("major",       extract_major_udf(F.col("text"))) \
               .withColumn("degree",      norm_degree_udf(F.col("text")))

        # --- แปลงอาร์เรย์ว่างให้เป็น null ---
        for c in ["major", "degree"]:
            df = df.withColumn(
                c,
                F.when(F.col(c).isNull() | (F.size(F.col(c)) == 0),
                       lit(None).cast(ArrayType(StringType())))
                 .otherwise(F.col(c))
            )

        # --- แปลง certificate ว่างเป็น null ---
        df = df.withColumn(
            "certificate",
            F.when(F.col("certificate").isNull() | (F.length(F.col("certificate")) == 0),
                   lit(None).cast(StringType()))
             .otherwise(F.col("certificate"))
        )

        # --- แปลง string ว่างทุกคอลัมน์ string ให้เป็น null ---
        for name, dtype in df.dtypes:
            if dtype == 'string':
                df = df.withColumn(
                    name,
                    F.when(F.length(F.trim(F.col(name))) == 0, lit(None).cast(StringType()))
                     .otherwise(F.col(name))
                )

        out[key] = DynamicFrame.fromDF(df, glueContext, f"{key}_classified")

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
AmazonS3_node1758203401191 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jobfit-g55-1/transformed/linkedin/linkedin_yes_skill/"], "recurse": True}, transformation_ctx="AmazonS3_node1758203401191")

# Script generated for node Custom Transform
CustomTransform_node1758203483111 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1758203401191": AmazonS3_node1758203401191}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1758206333846 = SelectFromCollection.apply(dfc=CustomTransform_node1758203483111, key=list(CustomTransform_node1758203483111.keys())[0], transformation_ctx="SelectFromCollection_node1758206333846")

# Script generated for node Drop Fields
DropFields_node1758206358997 = DropFields.apply(frame=SelectFromCollection_node1758206333846, paths=["qualifications", "text", "announce"], transformation_ctx="DropFields_node1758206358997")

# Script generated for node Rename Field
RenameField_node1758206563563 = RenameField.apply(frame=DropFields_node1758206358997, old_name="certificate", new_name="certification", transformation_ctx="RenameField_node1758206563563")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RenameField_node1758206563563, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758206777799", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758207170939 = glueContext.write_dynamic_frame.from_options(frame=RenameField_node1758206563563, connection_type="s3", format="json", connection_options={"path": "s3://jobfit-g55-1/transformed/linkedin/linkedin_all4/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1758207170939")

job.commit()