from fastapi import APIRouter
from settings import settings
import time
import logging

router = APIRouter(prefix="/Feat_05", tags=["Feat_05"])

logger = logging.getLogger("uvicorn")

# mariadb 데이터 가저오기 위한 spark 읽기 부분
def get_mariadb_df(spark, query: str):
  return spark.read.format("jdbc") \
    .option("url", f"{settings.jdbc_url}") \
    .option("dbtable", f"({query}) AS tmp") \
    .option("user", f"{settings.mariadb_user}") \
    .option("password", f"{settings.mariadb_password}") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .load()


# =================================================================
# 특정대상연도와 특정비교연도의 월별 이용객 증감률 DATA를 가져오기 위한 부분
# gr => Growth Rate
# =================================================================
def get_gr_data(spark, year1:int, year2:int): 
  query = f"""
    SELECT
      cur.`월별`,
      
      -- 전체 평균 (모든 월에 동일 값으로 붙음)
      total.`총 평균 승차 인원`,
      total.`총 평균 하차 인원`,
      total.`총 이용객 수`,

      -- 총 이용객 증감률
      IFNULL((cur.`총 이용객 수` - IFNULL(prev.`총 이용객 수`,0)) / NULLIF(cur.`총 이용객 수`,0) * 100, 0) AS `총 이용객 증감률`,
      -- 시간대별 증감률
      IFNULL((cur.`05~06` - IFNULL(prev.`05~06`,0)) / NULLIF(cur.`05~06`,0) * 100, 0) AS `05~06`,
      IFNULL((cur.`06~07` - IFNULL(prev.`06~07`,0)) / NULLIF(cur.`06~07`,0) * 100, 0) AS `06~07`,
      IFNULL((cur.`07~08` - IFNULL(prev.`07~08`,0)) / NULLIF(cur.`07~08`,0) * 100, 0) AS `07~08`,
      IFNULL((cur.`08~09` - IFNULL(prev.`08~09`,0)) / NULLIF(cur.`08~09`,0) * 100, 0) AS `08~09`,
      IFNULL((cur.`09~10` - IFNULL(prev.`09~10`,0)) / NULLIF(cur.`09~10`,0) * 100, 0) AS `09~10`,
      IFNULL((cur.`10~11` - IFNULL(prev.`10~11`,0)) / NULLIF(cur.`10~11`,0) * 100, 0) AS `10~11`,
      IFNULL((cur.`11~12` - IFNULL(prev.`11~12`,0)) / NULLIF(cur.`11~12`,0) * 100, 0) AS `11~12`,
      IFNULL((cur.`12~13` - IFNULL(prev.`12~13`,0)) / NULLIF(cur.`12~13`,0) * 100, 0) AS `12~13`,
      IFNULL((cur.`13~14` - IFNULL(prev.`13~14`,0)) / NULLIF(cur.`13~14`,0) * 100, 0) AS `13~14`,
      IFNULL((cur.`14~15` - IFNULL(prev.`14~15`,0)) / NULLIF(cur.`14~15`,0) * 100, 0) AS `14~15`,
      IFNULL((cur.`15~16` - IFNULL(prev.`15~16`,0)) / NULLIF(cur.`15~16`,0) * 100, 0) AS `15~16`,
      IFNULL((cur.`16~17` - IFNULL(prev.`16~17`,0)) / NULLIF(cur.`16~17`,0) * 100, 0) AS `16~17`,
      IFNULL((cur.`17~18` - IFNULL(prev.`17~18`,0)) / NULLIF(cur.`17~18`,0) * 100, 0) AS `17~18`,
      IFNULL((cur.`18~19` - IFNULL(prev.`18~19`,0)) / NULLIF(cur.`18~19`,0) * 100, 0) AS `18~19`,
      IFNULL((cur.`19~20` - IFNULL(prev.`19~20`,0)) / NULLIF(cur.`19~20`,0) * 100, 0) AS `19~20`,
      IFNULL((cur.`20~21` - IFNULL(prev.`20~21`,0)) / NULLIF(cur.`20~21`,0) * 100, 0) AS `20~21`,
      IFNULL((cur.`21~22` - IFNULL(prev.`21~22`,0)) / NULLIF(cur.`21~22`,0) * 100, 0) AS `21~22`,
      IFNULL((cur.`22~23` - IFNULL(prev.`22~23`,0)) / NULLIF(cur.`22~23`,0) * 100, 0) AS `22~23`,
      IFNULL((cur.`23~24` - IFNULL(prev.`23~24`,0)) / NULLIF(cur.`23~24`,0) * 100, 0) AS `23~24`,
      IFNULL((cur.`24~` - IFNULL(prev.`24~`,0)) / NULLIF(cur.`24~`,0) * 100, 0) AS `24~`

    FROM (
      -- 대상연도
      SELECT
        MONTH(`날짜`) AS `월별`,
        SUM(`총 이용객 수`) AS `총 이용객 수`,
        SUM(`05~06`) AS `05~06`, SUM(`06~07`) AS `06~07`,
        SUM(`07~08`) AS `07~08`, SUM(`08~09`) AS `08~09`,
        SUM(`09~10`) AS `09~10`, SUM(`10~11`) AS `10~11`,
        SUM(`11~12`) AS `11~12`, SUM(`12~13`) AS `12~13`,
        SUM(`13~14`) AS `13~14`, SUM(`14~15`) AS `14~15`,
        SUM(`15~16`) AS `15~16`, SUM(`16~17`) AS `16~17`,
        SUM(`17~18`) AS `17~18`, SUM(`18~19`) AS `18~19`,
        SUM(`19~20`) AS `19~20`, SUM(`20~21`) AS `20~21`,
        SUM(`21~22`) AS `21~22`, SUM(`22~23`) AS `22~23`,
        SUM(`23~24`) AS `23~24`, SUM(`24~`) AS `24~`
      FROM feat_05
      WHERE YEAR(`날짜`) = {year1}
      GROUP BY MONTH(`날짜`)
    ) cur

    LEFT JOIN (
      -- 비교연도
      SELECT
        MONTH(`날짜`) AS `월별`,
        SUM(`총 이용객 수`) AS `총 이용객 수`,
        SUM(`05~06`) AS `05~06`, SUM(`06~07`) AS `06~07`,
        SUM(`07~08`) AS `07~08`, SUM(`08~09`) AS `08~09`,
        SUM(`09~10`) AS `09~10`, SUM(`10~11`) AS `10~11`,
        SUM(`11~12`) AS `11~12`, SUM(`12~13`) AS `12~13`,
        SUM(`13~14`) AS `13~14`, SUM(`14~15`) AS `14~15`,
        SUM(`15~16`) AS `15~16`, SUM(`16~17`) AS `16~17`,
        SUM(`17~18`) AS `17~18`, SUM(`18~19`) AS `18~19`,
        SUM(`19~20`) AS `19~20`, SUM(`20~21`) AS `20~21`,
        SUM(`21~22`) AS `21~22`, SUM(`22~23`) AS `22~23`,
        SUM(`23~24`) AS `23~24`, SUM(`24~`) AS `24~`
      FROM feat_05
      WHERE YEAR(`날짜`) = {year2}
      GROUP BY MONTH(`날짜`)
    ) prev

    ON cur.`월별` = prev.`월별`

    -- 🔥 핵심: 전체 평균
    CROSS JOIN (
      SELECT
        SUM(`총 이용객 수`) AS `총 이용객 수`,
        ROUND(
          SUM(CASE WHEN `구분`='승차' THEN `총 이용객 수` END) /
          NULLIF(COUNT(CASE WHEN `구분`='승차' THEN 1 END),0)
        ,2) AS `총 평균 승차 인원`,
    
        ROUND(
          SUM(CASE WHEN `구분`='하차' THEN `총 이용객 수` END) /
          NULLIF(COUNT(CASE WHEN `구분`='하차' THEN 1 END),0)
        ,2) AS `총 평균 하차 인원`
    
      FROM feat_05
      WHERE YEAR(`날짜`) = {year1}
    ) total
    ORDER BY cur.`월별`
  """
  
  outputData = get_mariadb_df(spark, query)
  
  return outputData


# ============================================================
# feat-05 특정연도와 특정연도의 월별 총 이용객 증감률 데이터 가져오기
# ============================================================

@router.post("/metro_05_01")
async def get_gr_metro_data(year1:int, year2:int):
  from main import spark

  if spark is None:
    return {
      "status": False, 
      "error": "Spark session is not initialized. Please wait for startup."
    }
  
  try:
    # 데이터 가져와서 return값으로 보내주기 위해 스타트하는 시간
    st_fl = time.time()

    logger.info("[DB DATA 조회 시작]")

    # 실제 실행 (Spark Action)
    df = get_gr_data(spark, year1, year2).cache()
    # 가져온 데이터를 return값으로 보내주기 위해 스타트하는 시간
    st_tl = time.time()
    # result = df.toJSON().collect()
    result = df.toPandas().to_dict(orient="records")
    # 데이터 가져오는 시간 체크
    get_data_time(st_fl, st_tl, result)
    
    logger.info("[DB DATA 조회 완료]")

    return { "status": True, "data": result }

  # Spark / JDBC / DB 에러
  except Exception as e:
    error_msg = str(e)

    logger.error("ERROR 발생")
    logger.error(error_msg)

    return { "status": False, "error": error_msg }

  


# ================================================
# 데이터 가져오는 시간 체크
# ================================================

def get_data_time(st_fl, st_tl, result):
  
  total_time = time.time() - st_tl
  full_time = time.time() - st_fl

  logger.info("="*50)
  logger.info(f"[TOTAL TIME] : {total_time:.3f}")
  logger.info(f"[FULL TIME]  : {full_time:.3f}")
  logger.info(f"[DATA COUNT] : {len(result)}")
  logger.info("="*50)
  # logger.info(f"[DATA] : {result}")
  # logger.info("="*50)