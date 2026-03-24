import logging
from fastapi import APIRouter, HTTPException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum as _sum, regexp_replace, to_date
from sqlalchemy import create_engine, text
import pandas as pd # 결과 처리를 위해 추가
from settings import settings

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/feat_01", tags=["/feat_01"])

# 결과 확인용 DB 엔진
mariadb_engine = create_engine(settings.mariadb_url)

@router.post("/create_table")
def create_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS db_metro.feat_01 (
        id INT AUTO_INCREMENT PRIMARY KEY,

        년도 INT NOT NULL,
        월 INT NOT NULL,

        승차인원합계 BIGINT DEFAULT 0,
        하차인원합계 BIGINT DEFAULT 0,

        생성일 DATETIME DEFAULT CURRENT_TIMESTAMP,

        UNIQUE KEY uq_year_month (년도, 월),
        INDEX idx_year_month (년도, 월)
    );
    """

    try:
        with mariadb_engine.begin() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()

        return {"message": "테이블 생성 완료"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/data_processing")
def run_spark_feat01():
    try:
        logger.info("🚀 Spark 세션 시작 중...")
        spark = SparkSession.builder \
            .appName("feat_01_api") \
            .config("spark.sql.ansi.enabled", "false") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN") # 너무 많은 로그 방지
        spark.conf.set("spark.sql.ansi.enabled", "false")

        jdbc_url = settings.jdbc_url

        # 1. 데이터 로딩 (CAST 처리로 DATE/INT 변환 에러 방지)
        logger.info("📂 [1/5] MariaDB에서 원본 데이터를 읽어오는 중...")
        raw_query = """
        (SELECT 
            CAST(`날짜` AS CHAR) AS `날짜`, 
            CAST(`구분` AS CHAR) AS `구분`,
            CAST(`05~06` AS CHAR) AS `05~06`, CAST(`06~07` AS CHAR) AS `06~07`,
            CAST(`07~08` AS CHAR) AS `07~08`, CAST(`08~09` AS CHAR) AS `08~09`,
            CAST(`09~10` AS CHAR) AS `09~10`, CAST(`10~11` AS CHAR) AS `10~11`,
            CAST(`11~12` AS CHAR) AS `11~12`, CAST(`12~13` AS CHAR) AS `12~13`,
            CAST(`13~14` AS CHAR) AS `13~14`, CAST(`14~15` AS CHAR) AS `14~15`,
            CAST(`15~16` AS CHAR) AS `15~16`, CAST(`16~17` AS CHAR) AS `16~17`,
            CAST(`17~18` AS CHAR) AS `17~18`, CAST(`18~19` AS CHAR) AS `18~19`,
            CAST(`19~20` AS CHAR) AS `19~20`, CAST(`20~21` AS CHAR) AS `20~21`,
            CAST(`21~22` AS CHAR) AS `21~22`, CAST(`22~23` AS CHAR) AS `22~23`,
            CAST(`23~24` AS CHAR) AS `23~24`, CAST(`24~` AS CHAR) AS `24~`
         FROM seoul_metro 
         WHERE `날짜` REGEXP '^[0-9]') AS safe_feat
        """

        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", raw_query) \
            .option("user", settings.my_user) \
            .option("password", settings.my_pwd) \
            .option("driver", "org.mariadb.jdbc.Driver") \
            .load()

        # 2. 전처리: 날짜 정제 및 행별 합계 계산
        time_cols = [
            "05~06","06~07","07~08","08~09","09~10","10~11","11~12","12~13",
            "13~14","14~15","15~16","16~17","17~18","18~19","19~20","20~21",
            "21~22","22~23","23~24","24~"
        ]

        df = df.withColumn("날짜_fixed", to_date(regexp_replace(col("날짜"), r"[^0-9]", ""), "yyyyMMdd")) \
               .withColumn("년도", year(col("날짜_fixed"))) \
               .withColumn("월", month(col("날짜_fixed")))

        row_total_expr = sum([col(c).cast("long") for c in time_cols])
        df = df.withColumn("행별_합계", row_total_expr)

        # 3. Pivot 처리 (승차/하차 컬럼 분리)
        logger.info("🔄 [3/5] 승차/하차 데이터를 컬럼으로 전환(Pivot) 중...")
        pivot_df = df.groupBy("년도", "월") \
                     .pivot("구분", ["승차", "하차"]) \
                     .agg(_sum("행별_합계")) \
                     .withColumnRenamed("승차", "승차인원합계") \
                     .withColumnRenamed("하차", "하차인원합계") \
                     .na.fill(0)

        # 4. MariaDB feat_01 테이블에 저장
        insert_count = pivot_df.count()
        logger.info(f"💾 [4/5] MariaDB 적재 시작... (예정 건수: {insert_count}건)")

        pivot_df.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "feat_01") \
            .option("user", settings.my_user) \
            .option("password", settings.my_pwd) \
            .option("driver", "org.mariadb.jdbc.Driver") \
            .option("quoteIdentifiers", "false") \
            .mode("append") \
            .save()

        # 5. [최종 단계] 적재 후 DB 실시간 개수 확인
        with mariadb_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM feat_01"))
            total_db_count = result.scalar()

        logger.info(f"✅ [5/5] 작업 완료! 현재 feat_01 총 레코드 수: {total_db_count}개")
        
        return {
            "status": "success",
            "inserted_count": insert_count,
            "total_db_count": total_db_count,
            "message": "데이터 집계 및 적재가 완료되었습니다."
        }

    except Exception as e:
        logger.error(f"❌ 작업 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/summary")
def get_metro_summary():
    try:
        # 1. DB에서 feat_01 테이블 데이터 조회 (최신 데이터순)
        query = "SELECT 년도, 월, 승차인원합계, 하차인원합계 FROM feat_01 ORDER BY 년도 DESC, 월 DESC"
        
        with mariadb_engine.connect() as conn:
            # pandas를 사용하면 JSON 변환이 매우 편리합니다.
            df = pd.read_sql(query, conn)
            
        # 2. DataFrame을 JSON(리스트 형태)으로 변환
        # orient="records"를 사용하면 [{년도: 2024, 월: 3, ...}, ...] 형식으로 나옵니다.
        result = df.to_dict(orient="records")
        
        return {
            "status": "success",
            "data": result,
            "total_count": len(result)
        }

    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=500, detail="데이터를 불러오는데 실패했습니다.")