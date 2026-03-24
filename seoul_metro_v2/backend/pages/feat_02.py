import logging
from fastapi import APIRouter, HTTPException, Query
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, row_number, sum as _sum, regexp_replace, to_date
from pyspark.sql.window import Window
from sqlalchemy import create_engine, text
from typing import Optional
from settings import settings

logger = logging.getLogger(__name__)
mariadb_engine = create_engine(settings.mariadb_url, connect_args={"local_infile": 1})

router = APIRouter(prefix="/feat_02", tags=["feat_02"])

@router.post("/create_table")
def create_table():
    create_table_sql = """
    CREATE TABLE feat_02 (
    날짜 DATE NOT NULL,
    역명 VARCHAR(200) NOT NULL,
    시간대 VARCHAR(10) NOT NULL,   -- '05~06', '08~09', 'ALL'
    기준 VARCHAR(10) NOT NULL,     -- '승차', '하차'
    인원 INT NOT NULL,
    순위 INT NOT NULL,

    -- 중복 방지 (핵심)
    PRIMARY KEY (날짜, 역명, 시간대, 기준),

    -- 조회 성능 인덱스
    INDEX idx_date (날짜),
    INDEX idx_time (시간대),
    INDEX idx_type (기준),
    INDEX idx_rank (순위),

    -- 복합 조회 최적화 (차트용 핵심)
    INDEX idx_chart (날짜, 시간대, 기준, 순위)

    ) ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_unicode_ci;
    """

    try:
        with mariadb_engine.begin() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()

        return {"message": "테이블 생성 완료"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/data_processing")
def feat_02_spark_processing():
    spark = None
    try:
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.ansi.enabled", "false")
       
        # 세션이 잘 살아있는지 확인용 로그
        logger.info(f"Using Spark Session: {spark.sparkContext.appName}")
        

        jdbc_url = settings.jdbc_url
        properties = {
            "user": settings.my_user,
            "password": settings.my_pwd,
            "driver": "org.mariadb.jdbc.Driver"
        }

        # 1. 데이터 로드
        raw_query = """
        (SELECT 
            s.`날짜`, s.`구분`, m.`역명`,
            s.`05~06`, s.`06~07`, s.`07~08`, s.`08~09`, s.`09~10`, 
            s.`10~11`, s.`11~12`, s.`12~13`, s.`13~14`, s.`14~15`, 
            s.`15~16`, s.`16~17`, s.`17~18`, s.`18~19`, s.`19~20`, 
            s.`20~21`, s.`21~22`, s.`22~23`, s.`23~24`, s.`24~`
         FROM seoul_metro s
         INNER JOIN metro_line m ON s.역번호 = m.역번호
         WHERE s.`날짜` REGEXP '^[0-9]') AS joined_data
        """
        df = spark.read.jdbc(url=jdbc_url, table=raw_query, properties=properties).cache()
        load_count = df.count()
        logger.info(f"📊 [로드 완료] 원본 데이터 수: {load_count:,}건")

        # 2. 날짜 정제
        df = df.withColumn("날짜_fixed", to_date(regexp_replace(col("날짜"), r"[^0-9]", ""), "yyyyMMdd"))

        # 3. Unpivot (시간대 컬럼들을 세로로 펼치기)
        logger.info("🔄 [2/6] 시간대별 데이터 Unpivot(세로로 펼치기) 진행 중...")
        time_cols = [
            "'05~06', `05~06`","'06~07', `06~07`","'07~08', `07~08`","'08~09', `08~09`","'09~10', `09~10`",
            "'10~11', `10~11`","'11~12', `11~12`","'12~13', `12~13`","'13~14', `13~14`","'14~15', `14~15`",
            "'15~16', `15~16`","'16~17', `16~17`","'17~18', `17~18`","'18~19', `18~19`","'19~20', `19~20`",
            "'20~21', `20~21`","'21~22', `21~22`","'22~23', `22~23`","'23~24', `23~24`","'24~', `24~`"
        ]
        stack_expr = f"stack(20, {', '.join(time_cols)}) as (`시간대`, `인원`)"
        
        unpivoted_df = df.selectExpr(
            "`날짜_fixed` as `날짜` ", 
            "`역명` as `역명` ", 
            stack_expr,      # 시간대, 인원 생성
            "`구분` as `기준` "
        ).withColumn("인원", col("인원").cast("int")).na.fill(0).cache()

        unpivot_count = unpivoted_df.count() # 변수 정의 완료
        logger.info(f"📊 [Unpivot 완료] 펼쳐진 총 행 수: {unpivot_count:,}건")

        # 4. 'ALL' 시간대 데이터 생성
        daily_all_df = unpivoted_df.groupBy("날짜", "역명", "기준") \
                                   .agg(_sum("인원").alias("인원")) \
                                   .withColumn("시간대", expr("CAST('ALL' AS STRING)")) \
                                   .select("날짜", "역명", "시간대", "기준", "인원") # 순서 고정

        all_count = daily_all_df.count() # 변수 정의 완료
        logger.info("🏆 [4/6] 시간대별/일일 TOP 50 순위 계산 중...")

        # 5. 데이터 합치기 및 순위 계산
        unpivoted_target = unpivoted_df.select("날짜", "역명", "시간대", "기준", "인원")
        final_df = unpivoted_target.unionByName(daily_all_df, allowMissingColumns=True)

        # [핵심] 인원이 0인 경우를 위해 역명을 보조 정렬로 추가 (Tie-break)
        window_spec = Window.partitionBy("날짜", "시간대", "기준") \
                            .orderBy(col("인원").desc(), col("역명").asc())
        
        # [핵심] DB 컬럼 순서(날짜, 역명, 시간대, 기준, 인원, 순위)와 100% 일치시킴
        ranked_df = final_df.withColumn("순위", row_number().over(window_spec)) \
                            .filter(col("순위") <= 50) \
                            .select("날짜", "역명", "시간대", "기준", "인원", "순위") \
                            .cache()

        final_count = ranked_df.count()
        logger.info(f"📊 [랭킹 완료] 최종 적재 대상 건수(TOP 50만 추출): {final_count:,}건")

        # 6. 적재
        ranked_df.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "feat_02") \
            .option("user", settings.my_user) \
            .option("password", settings.my_pwd) \
            .option("driver", "org.mariadb.jdbc.Driver") \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()

        
        # 7. 최종 확인
        logger.info(f"✅ [6/6] Feat_02 모든 작업 완료! 최종 {final_count:,}건이 적재되었습니다.")

        # 캐시 해제
        df.unpersist(); unpivoted_df.unpersist(); ranked_df.unpersist()

        return {
            "status": "success",
            "counts": {"original": load_count, "unpivoted": unpivot_count, "daily_all": all_count, "final_top50": final_count}
        }

    except Exception as e:
        logger.error(f"오류 발생: {e}")
    finally:
        if spark:
            # 작업이 끝나면 명시적으로 세션을 닫아 리소스를 반납합니다.
            spark.stop() 
            logger.info("Spark 리소스 반납 완료")

@router.get("/metro_02")
def get_metro_rankings(
    date: str = Query(..., description="조회 날짜 (YYYY-MM-DD)"),
    time_range: str = Query("ALL", description="시간대 (05~06, ALL 등)"),
    type: str = Query("승차", description="기준 (승차/하차)"),
    top_n: int = Query(10, ge=10, le=50, description="조회할 순위 범위")
):
    try:
        # 사용자가 선택한 TOP N(10/20/50)에 맞춰 데이터를 가져오는 쿼리
        query = text("""
            SELECT 역명, 인원, 순위
            FROM feat_02
            WHERE 날짜 = :date 
              AND 시간대 = :time_range 
              AND 기준 = :type
              AND 순위 <= :top_n
            ORDER BY 순위 ASC
        """)
        
        with mariadb_engine.connect() as conn:
            result = conn.execute(query, {
                "date": date, 
                "time_range": time_range, 
                "type": type, 
                "top_n": top_n
            })
            
            # data = [dict(row) for row in result]
            # 💡 이 부분이 핵심 수정 사항입니다!
            data = [row._asdict() for row in result]
            
        if not data:
            return {"message": "해당 조건의 역 정보가 없습니다.", "data": []}
            
        return {"status": "success", "data": data}

    except Exception as e:
        logger.error(f"조회 오류: {e}")
        raise HTTPException(status_code=500, detail="데이터 조회 중 오류가 발생했습니다.")
    
@router.get("/available-times")
def get_available_times():
    # 전처리 시 사용했던 시간대 리스트를 반환
    return [
        "ALL", "05~06", "06~07", "07~08", "08~09", "09~10", 
        "10~11", "11~12", "12~13", "13~14", "14~15", "15~16", 
        "16~17", "17~18", "18~19", "19~20", "20~21", "21~22", 
        "22~23", "23~24", "24~"
    ]