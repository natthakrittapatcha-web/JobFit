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
    # EN
    r"bachelor[’']?s\s+degree",
    r"master[’']?s\s+degree",
    r"ph\.?d|doctorate",
    r"associate[’']?s\s+degree",
    r"high\s*school\s*diploma",
    r"diploma",
    r"ged\b",

    # EN (extra - any field/discipline/major)
    r"bachelor[’']?s\s+degree\s+in\s+any\s+(field|discipline|major|background)",
    r"any\s+bachelor[’']?s\s+degree",

    # TH
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
        # ทั่วไป
    "computer science","information technology","data science","statistics","mathematics",
    "marketing","business","finance","accounting","economics","engineering",
    "mechanical engineering","industrial engineering","electrical engineering",
    "software engineering","civil engineering","structural engineering",
    "environmental engineering","chemical engineering","aeronautical engineering",
    "computer engineering","materials science","robotics","control engineering",
    "electronics","communications","information theory","systems","networking",
    "artificial intelligence","machine learning","computer vision","databases",
    "programming languages","human-computer interaction","security and privacy",
    "biochemistry","molecular biology","cell biology","genetics","immunology","microbiology",
    "astronomy","physics","applied physics","theoretical physics","quantum theory",
    "geology","geophysics","meteorology","climate science","hydrology","oceanography",
    "archaeology","physical geography","ecology","agricultural science","soil science",
    "entomology","parasitology","health sciences","nutrition",

    # สายแพทย์/สุขภาพ
    "medicine","doctor of medicine","mbbs","md",
    "nursing","bachelor of nursing science","bnsc",
    "dentistry","doctor of dental surgery","dds","dmd",
    "pharmacy","pharmaceutical sciences",
    "veterinary medicine","doctor of veterinary medicine","dvm",
    "public health","health administration","health promotion",
    "physiotherapy","physical therapy","rehabilitation medicine",
    "occupational therapy","speech-language pathology","audiology",
    "medical technology","clinical laboratory science","laboratory medicine",
    "radiologic technology","medical imaging","radiography",
    "biomedical science","biomedical sciences","biomedical engineering","clinical engineering",
    "biotechnology","medical biotechnology",
    "nutrition and dietetics","dietetics",
    "health informatics","medical informatics",
    "epidemiology","biostatistics","global health",
    "anatomical sciences","pathology","medical microbiology","virology","toxicology","neuroscience",
    # สาขาเฉพาะทางแพทย์ (เจอบ่อยใน JD)
    "anesthesiology","surgery","internal medicine","pediatrics","obstetrics and gynecology",
    "dermatology","psychiatry","family medicine","orthopedics","ophthalmology",
    "otolaryngology","cardiology","neurology","radiology","oncology","urology",

        # TH
    "วิทยาการคอมพิวเตอร์","เทคโนโลยีสารสนเทศ","สารสนเทศศาสตร์","วิทยาการข้อมูล","ดาต้าวิทยา",
    "สถิติ","สถิติประยุกต์","คณิตศาสตร์","การตลาด","บริหารธุรกิจ","ธุรกิจ","การเงิน","บัญชี",
    "เศรษฐศาสตร์","วิศวกรรมเครื่องกล","วิศวกรรมอุตสาหการ","วิศวกรรมไฟฟ้า","วิศวกรรมซอฟต์แวร์",
    "วิศวกรรมคอมพิวเตอร์","วิศวกรรมโยธา","วิศวกรรมสิ่งแวดล้อม","วิศวกรรมเคมี","วิศวกรรมอากาศยาน",
    "อิเล็กทรอนิกส์","โทรคมนาคม","หุ่นยนต์","ควบคุม","วัสดุศาสตร์","เทคโนโลยีวัสดุ",
    "ชีวเคมี","ชีววิทยาโมเลกุล","ชีววิทยาเซลล์","พันธุศาสตร์","ภูมิคุ้มกันวิทยา","จุลชีววิทยา",
    "ฟิสิกส์","ฟิสิกส์ประยุกต์","ฟิสิกส์ทฤษฎี","ดาราศาสตร์","ธรณีวิทยา","ธรณีฟิสิกส์",
    "อุตุนิยมวิทยา","ภูมิอากาศ","อุทกวิทยา","สมุทรศาสตร์","โบราณคดี","ภูมิศาสตร์กายภาพ",
    "นิเวศวิทยา","เกษตรศาสตร์","วิทยาศาสตร์ดิน","กีฏวิทยา","ปรสิตวิทยา","วิทยาศาสตร์สุขภาพ","โภชนาการ",

    # สายแพทย์/สุขภาพ
    "แพทยศาสตร์","เวชศาสตร์","คณะแพทยศาสตร์","แพทย์",
    "พยาบาลศาสตร์","พยาบาล","ผดุงครรภ์",
    "ทันตแพทยศาสตร์","ทันตแพทย์",
    "เภสัชศาสตร์","เภสัชกร","วิทยาศาสตร์เภสัชกรรม",
    "สัตวแพทยศาสตร์","สัตวแพทย์",
    "สาธารณสุขศาสตร์","สาธารณสุข","อนามัยชุมชน","บริหารสาธารณสุข",
    "กายภาพบำบัด","เวชศาสตร์ฟื้นฟู","กิจกรรมบำบัด",
    "เทคนิคการแพทย์","วิทยาศาสตร์การแพทย์ทางห้องปฏิบัติการ","ห้องปฏิบัติการทางการแพทย์",
    "รังสีเทคนิค","เวชศาสตร์รังสี","เทคโนโลยีรังสี","เวชศาสตร์นิวเคลียร์",
    "โภชนวิทยา","นักกำหนดอาหาร","ชีวเวชศาสตร์","วิทยาศาสตร์ชีวการแพทย์","ชีวการแพทย์",
    "เทคโนโลยีชีวภาพทางการแพทย์","วิศวกรรมชีวการแพทย์",
    "สุขภาพดิจิทัล","สารสนเทศสุขภาพ","เวชสารสนเทศ",
    "ระบาดวิทยา","ชีวสถิติ","สุขภาพโลก",
    "จุลชีววิทยาการแพทย์","ไวรัสวิทยา","พยาธิวิทยา","กายวิภาคศาสตร์","สรีรวิทยา","ประสาทวิทยา","พิษวิทยา",
    # สาขาเฉพาะทางแพทย์
    "ศัลยศาสตร์","อายุรศาสตร์","กุมารเวชศาสตร์","สูติศาสตร์นรีเวชวิทยา",
    "ผิวหนัง","จิตเวชศาสตร์","เวชศาสตร์ครอบครัว","ออร์โธปิดิกส์",
    ]

    CERT_PATTERNS = [
    # --- Standard English tests with scores ---
    r"toeic\s*\d{2,4}",                       # TOEIC พร้อมคะแนน (เช่น TOEIC 600, TOEIC 850)
    r"toefl\s*(ibt|pbt|cbt)?\s*\d{2,4}",      # TOEFL + ชนิดการสอบ + คะแนน
    r"ielts\s*[\d\.]+",                       # IELTS พร้อมคะแนน (เช่น IELTS 6.5)

    # --- ชื่อข้อสอบภาษา/มาตรฐาน ---
    r"toeic|toefl|ielts",                     # TOEIC, TOEFL, IELTS

    # --- Thai keywords ---
    r"ใบรับรอง|ประกาศนียบัตร|ใบอนุญาต"     # คำไทยที่พบบ่อยุญาตาต"
    ]

    SKILL_TERMS = [
    "python","sql","excel","power bi","tableau","git","notion",
    "google analytics","facebook ads","tiktok ads","seo","sem","crm",
    "ux","ui","wireframe","prototype","figma","agile","scrum","kanban",
    "product roadmap","a/b testing","cohort analysis",
    "Analytical Skills","Data Analysis","Engineering","English","Logistics Management","Mechatronics","Microsoft Excel","Problem Solving","Robotics","Supply Chain Management",
    "Communication","Failure Modes","Key Performance Indicators","Manufacturing","Manufacturing Processes","Production Engineering","Production Processes","Quality Control",
    "Computer Science","E-Commerce","Jira","Product Development","Product Management","Product Road Mapping","Sprint Planning","User Experience (UX)",
    "Analytics","Business Innovation","Competitive Analysis","Project Management","User Acceptance Testing","User Stories",
    "Business Administration","Business Development","Business Operations","Customer Experience","Economics","Google Sheets","Office Equipment","Operations","Operations Processes",
    "Banquets","Budgeting","Corrective Actions","Environmental, Social, and Governance (ESG)","Event Planning","Events","Multi-cultural Environment","Organization Skills",
    "Ad Serving","Advertising Campaigns","Marketing Design","Performance Improvement","Performance Tuning","Sales Target Management","Strategic Insights",
    "Business Value","Decision-Making","Interactive Web","Iterative","Software Product Management","Stakeholder Management",
    "Attention to Detail","Business Metrics","Daily Operations","Data Analytics","Presentation Skills","Oracle Database",
    "Advertising","Digital Marketing","Display Advertising","Marketing","Marketing Campaign Management","Negotiation","Social Media","Social Media Advertising",
    "Demand Planning","Freight Management","Product Lifecycle Management","Discord","Product Strategy","Strategy",
    "Business Workflows","Customer Relationship Management (CRM)","Databases","Process Modeling","Visio",
    "Critical Thinking","Event Management","Interpersonal Communication","Interpersonal Skills","Microsoft Office",
    "Client Presentation","Executive Management","Project Implementation","Business Planning","Business Strategy","Game Mechanics","Strategic Communications","Strategic Planning",
    "Campaigns","Go-to-Market Strategy","Key Metrics","Marketing Campaigns","Sales Operations","Sales Processes",
    "Accounting","Import Export","Perfume","Procurement","Report Preparation","Retail",
    "Email Marketing","Multitasking","Search Engine Marketing (SEM)","Search Engine Optimization (SEO)",
    "Business Analysis","Business Process","Business Requirements","Requirements Gathering","Sales Performance",
    "Microsoft PowerPoint","Troubleshooting","Business Insights","Content Marketing","Editing","Thai","Web Content Writing","Writing",
    "Branding","Financial Management","Organizational Development","Organizational Management","Sales","Sales Management",
    "Marketing Operations","Strategic Business","Business","Demographics","Financial Markets","Geography","Market Share","Working Experience","Competitive Intelligence","Strategic Initiatives",
    "Account Planning","Market Planning","Performance Reviews","Program Development","Program Management",
    "Back-End Web Development","Cascading Style Sheets (CSS)","Front-End Development","Full-Stack Development","HTML5","JavaScript","Stack",
    "Human Resources (HR)","Online Marketing","Pay Per Click (PPC)","Web Analytics","Google Docs","Programming","Strategic Thinking","Workload Prioritization","Written Communication",
    "Project Management Office (PMO)","Word","information technology","data science","statistics","mathematics","finance","mechanical engineering","industrial engineering","electrical engineering",
    "software engineering","law","political science","public policy","international relations","operations management","supply chain",
    "systems","networking","artificial intelligence","machine learning","computer vision","programming languages","human-computer interaction",
    "quantum computing","biological computing","security and privacy","pure mathematics","applied mathematics","theoretical mechanics","applied mechanics",
    "theoretical physics","operational research","astronomy","physics","applied physics","astrophysics","biophysics","computational physics","condensed matter",
    "nanomaterials","cosmology","crystallography","elementary particle physics","gravitation","interstellar medium","lasers","optoelectronics","low temperature physics",
    "magnetism","mathematical physics","nuclear physics","atomic physics","molecular physics","planetary science","plasma physics","quantum theory",
    "semiconductors","solar physics","statistical physics","experimental physics","chemistry","applied chemistry","theoretical chemistry","organic chemistry",
    "inorganic chemistry","physical chemistry","biological chemistry","materials chemistry","technology","instrumentation","materials science","fluid dynamics","general engineering",
    "civil engineering","structural engineering","environmental engineering","chemical engineering","aeronautical engineering","electronics","space technology",
    "communications","information theory","computer engineering","control engineering","nuclear technology","electric power","earth sciences",
    "mineralogy","geology","geophysics","geochemistry","atmospheric physics","meteorology","climate science","hydrology","oceanography","limnology","archaeology",
    "physical geography","biochemistry","structural biology","molecular biology","cell biology","genetics","developmental biology","microbiology","cytogenetics",
    "anatomy","physiology","neurosciences","endocrinology","pharmacology","experimental psychology","behavioural neuroscience","organismal biology","zoology","botany","ethology","ecology","taxonomy",
    "population genetics","agricultural science","soil science","entomology","paleozoology","health sciences","nutrition","medical statistics","medical instrumentation",
    "medicine","doctor of medicine","mbbs","md","nursing","bachelor of nursing science","bnsc",
    "dentistry","doctor of dental surgery","dds","dmd","pharmacy","pharmaceutical sciences",
    "veterinary medicine","doctor of veterinary medicine","dvm","public health","health administration","health promotion",
    "physiotherapy","physical therapy","rehabilitation medicine","occupational therapy","speech-language pathology","audiology",
    "medical technology","clinical laboratory science","laboratory medicine","radiologic technology","medical imaging","radiography",
    "biomedical science","biomedical sciences","biomedical engineering","clinical engineering","biotechnology","medical biotechnology",
    "nutrition and dietetics","dietetics","health informatics","medical informatics","biostatistics","global health",
    "anatomical sciences","pathology","medical microbiology","virology","toxicology","midwifery","emergency medical services","paramedic science",
    "anesthesiology","surgery","internal medicine","pediatrics","obstetrics and gynecology",
    "dermatology","psychiatry","family medicine","orthopedics","ophthalmology","otolaryngology","cardiology","neurology","radiology","oncology","urology",
    "driving skills","driver license","driving license"

    # —— Thai skill terms ——
    "การวิเคราะห์ข้อมูล","การนำเสนอ","การสื่อสาร","ทำงานเป็นทีม",
    "แก้ปัญหา","คิดเชิงวิพากษ์","วิเคราะห์เชิงสถิติ","จัดการโครงการ",
    "วางแผนกลยุทธ์","บริหารเวลา","ภาษาอังกฤษสื่อสาร",
    "วิทยาการคอมพิวเตอร์","คอมพิวเตอร์","เทคโนโลยีสารสนเทศ","สารสนเทศศาสตร์",
    "วิทยาการข้อมูล","ดาต้าวิทยา","สถิติ","สถิติประยุกต์","คณิตศาสตร์",
    "การตลาด","บริหารธุรกิจ","ธุรกิจ","การเงิน","บัญชี","เศรษฐศาสตร์",
    "วิศวกรรมเครื่องกล","วิศวกรรมอุตสาหการ","วิศวกรรมไฟฟ้า","วิศวกรรมซอฟต์แวร์",
    "วิศวกรรมคอมพิวเตอร์","วิศวกรรมโยธา","วิศวกรรมสิ่งแวดล้อม","วิศวกรรมเคมี",
    "วิศวกรรมอากาศยาน","อิเล็กทรอนิกส์","โทรคมนาคม","หุ่นยนต์","ควบคุม",
    "วิทยาศาสตร์ชีวภาพ","ชีววิทยาโมเลกุล","ชีวเคมี","วัสดุศาสตร์","เทคโนโลยีวัสดุ",
    "ฟิสิกส์","ฟิสิกส์ประยุกต์","ดาราศาสตร์","ธรณีวิทยา","ธรณีฟิสิกส์",
    "อุตุนิยมวิทยา","ภูมิอากาศ","อุทกวิทยา","สมุทรศาสตร์","ภูมิศาสตร์กายภาพ",
    "เคมี","เคมีอินทรีย์","เคมีอนินทรีย์","เคมีวิเคราะห์","เคมีฟิสิกส์",
    "รัฐศาสตร์","นโยบายสาธารณะ","ความสัมพันธ์ระหว่างประเทศ","กฎหมาย","นิติศาสตร์",
    "โลจิสติกส์","ซัพพลายเชน","การจัดการปฏิบัติการ",

    # --- สายแพทย์/สุขภาพ ---
    "แพทยศาสตร์","เวชศาสตร์","คณะแพทยศาสตร์","แพทย์",
    "พยาบาลศาสตร์","พยาบาล","ผดุงครรภ์",
    "ทันตแพทยศาสตร์","ทันตแพทย์",
    "เภสัชศาสตร์","เภสัชกร","วิทยาศาสตร์เภสัชกรรม",
    "สัตวแพทยศาสตร์","สัตวแพทย์",
    "สาธารณสุขศาสตร์","สาธารณสุข","อนามัยชุมชน","บริหารสาธารณสุข",
    "กายภาพบำบัด","เวชศาสตร์ฟื้นฟู","กายภาพบำบัดและฟื้นฟู",
    "กิจกรรมบำบัด",
    "เทคนิคการแพทย์","วิทยาศาสตร์การแพทย์ทางห้องปฏิบัติการ","ห้องปฏิบัติการทางการแพทย์",
    "รังสีเทคนิค","เวชศาสตร์รังสี","เทคโนโลยีรังสี","เวชศาสตร์นิวเคลียร์",
    "โภชนาการ","โภชนวิทยา","นักกำหนดอาหาร","โภชนาการและการกำหนดอาหาร",
    "ชีวเวชศาสตร์","วิทยาศาสตร์ชีวการแพทย์","ชีวการแพทย์","เทคโนโลยีชีวภาพทางการแพทย์",
    "วิศวกรรมชีวการแพทย์",
    "สุขภาพดิจิทัล","สารสนเทศสุขภาพ","เวชสารสนเทศ",
    "ระบาดวิทยา","ชีวสถิติ","สุขภาพโลก",
    "จุลชีววิทยาการแพทย์","ภูมิคุ้มกันวิทยา","ปรสิตวิทยา","ไวรัสวิทยา","พยาธิวิทยา",
    "กายวิภาคศาสตร์","สรีรวิทยา","ประสาทวิทยา","พิษวิทยา",
    "เวชศาสตร์ฉุกเฉิน","เจ้าหน้าที่การแพทย์ฉุกเฉิน",

    # สาขาเฉพาะทางแพทย์
    "ศัลยศาสตร์","อายุรศาสตร์","กุมารเวชศาสตร์","สูติศาสตร์นรีเวชวิทยา",
    "จิตเวชศาสตร์","เวชศาสตร์ครอบครัว","ออร์โธปิดิกส์","สื่อสารภาษาอังกฤษได้",

    # ทักษะทั่วไป
    "ทักษะการขับขี่","มีใบขับขี่","ภาษาอังกฤษ"
    ]


    # -------------------------
    # 2) Compile Regex
    # -------------------------
    DEGREE_RX = re.compile(r"(?:" + r"|".join(DEGREE_PATTERNS) + r")", re.IGNORECASE)
    CERT_RX   = re.compile(r"(?:" + r"|".join(CERT_PATTERNS)   + r")", re.IGNORECASE)
    MAJOR_RX  = re.compile("|".join([re.escape(t) for t in MAJOR_TERMS]), re.IGNORECASE)
    SKILL_RX  = re.compile("|".join([re.escape(t) for t in SKILL_TERMS]), re.IGNORECASE)

    SENT_SPLIT_RX = re.compile(r"(?:\n+|[•\u2022·\u2023\u25E6\u2043\-*]+|\s{2,}|(?<=[\.!?])\s+)")

    # -------------------------
    # 3) Helper functions
    # -------------------------
    def split_sentences(text):
        if not text:
            return []
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
    def extract_cert_udf_fn(text):   return _extract(CERT_RX, split_sentences(text))
    def extract_skill_udf_fn(text):  return _extract(SKILL_RX, split_sentences(text))

    # ถ้าเจอ major แต่ไม่พบ degree → ถือเป็น ปริญญาตรี
    def normalize_degree_from_major_udf_fn(text):
        deg = extract_degree_udf_fn(text)
        maj = extract_major_udf_fn(text)
        if (not deg) and maj:
            return ["Bachelor’s Degree"]
        return deg or []

    # -------------------------
    # 4) Register UDFs
    # -------------------------
    extract_degree_udf  = udf(extract_degree_udf_fn,  ArrayType(StringType()))
    extract_major_udf   = udf(extract_major_udf_fn,   ArrayType(StringType()))
    extract_cert_udf    = udf(lambda text: ",".join(extract_cert_udf_fn(text)) if extract_cert_udf_fn(text) else None, StringType())
    extract_skill_udf   = udf(extract_skill_udf_fn,   ArrayType(StringType()))
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
            # แปลงสตริงว่างของ text ให้เป็น null
            df = df.withColumn(
                'text',
                F.when(F.length(F.trim(F.col('text'))) == 0, F.lit(None).cast(StringType()))
                 .otherwise(F.col('text').cast(StringType()))
            )

        # Extract
        df = df.withColumn("certificate", extract_cert_udf(F.col("text"))) \
               .withColumn("skill",       extract_skill_udf(F.col("text"))) \
               .withColumn("major",       extract_major_udf(F.col("text"))) \
               .withColumn("degree",      norm_degree_udf(F.col("text")))

        # --- แปลงอาร์เรย์ว่างให้เป็น null ---
        for c in ["skill", "major", "degree"]:
            df = df.withColumn(
                c,
                F.when(F.col(c).isNull() | (F.size(F.col(c)) == 0),
                       F.lit(None).cast(ArrayType(StringType())))
                 .otherwise(F.col(c))
            )

        # --- เซ็ตสตริงว่างในทุกคอลัมน์ชนิด string ให้เป็น null (เผื่อมีคอลัมน์อื่น ๆ) ---
        for name, dtype in df.dtypes:
            if dtype == 'string':
                df = df.withColumn(
                    name,
                    F.when(F.length(F.trim(F.col(name))) == 0, F.lit(None).cast(StringType()))
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
AmazonS3_node1758211656689 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jobfit-g55-1/transformed/jobsdb/jodsdb_not_skill/"], "recurse": True}, transformation_ctx="AmazonS3_node1758211656689")

# Script generated for node Custom Transform
CustomTransform_node1758211733028 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1758211656689": AmazonS3_node1758211656689}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1758212292671 = SelectFromCollection.apply(dfc=CustomTransform_node1758211733028, key=list(CustomTransform_node1758211733028.keys())[0], transformation_ctx="SelectFromCollection_node1758212292671")

# Script generated for node Drop Fields
DropFields_node1758212321309 = DropFields.apply(frame=SelectFromCollection_node1758212292671, paths=["skills", "qualifications", "text"], transformation_ctx="DropFields_node1758212321309")

# Script generated for node Rename Field
RenameField_node1758214029720 = RenameField.apply(frame=DropFields_node1758212321309, old_name="certificate", new_name="certification", transformation_ctx="RenameField_node1758214029720")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RenameField_node1758214029720, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758211647141", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758214440971 = glueContext.write_dynamic_frame.from_options(frame=RenameField_node1758214029720, connection_type="s3", format="json", connection_options={"path": "s3://jobfit-g55-1/transformed/jobsdb/jobsdb_all4/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1758214440971")

job.commit()