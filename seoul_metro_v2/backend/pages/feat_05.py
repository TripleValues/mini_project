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
      cur.`월별`, cur.`역명`, cur.`구분타입`,
      total.`총 평균 승차 인원`, total.`총 평균 하차 인원`,
      cur.`총 이용객 수` AS `월별 이용객 수`,
      IFNULL(
        (cur.`총 이용객 수` - IFNULL(prev.`총 이용객 수`,0)) 
        / NULLIF(prev.`총 이용객 수`,0) * 100
      ,0) AS `총 이용객 증감률`
    FROM (
      -- 대상연도 (역별 + 성수기/비성수기)
      SELECT
        MONTH(`날짜`) AS `월별`, `역명`,
        `성수기구분` AS `구분타입`,
        SUM(`총 이용객 수`) AS `총 이용객 수`
      FROM feat_05
      WHERE YEAR(`날짜`) = {year1}
      GROUP BY MONTH(`날짜`), `역명`, `성수기구분`
      UNION ALL
      -- 대상연도 전체 (역별)
      SELECT
        MONTH(`날짜`) AS `월별`, `역명`,
        '전체' AS `구분타입`,
        SUM(`총 이용객 수`) AS `총 이용객 수`
      FROM feat_05
      WHERE YEAR(`날짜`) = {year1}
      GROUP BY MONTH(`날짜`), `역명`
    ) cur
    LEFT JOIN (
      -- 비교연도 동일 구조
      SELECT
        MONTH(`날짜`) AS `월별`, `역명`,
        `성수기구분` AS `구분타입`,
        SUM(`총 이용객 수`) AS `총 이용객 수`
      FROM feat_05
      WHERE YEAR(`날짜`) = {year2}
      GROUP BY MONTH(`날짜`), `역명`, `성수기구분`
      UNION ALL
      SELECT
        MONTH(`날짜`) AS `월별`, `역명`,
        '전체' AS `구분타입`,
        SUM(`총 이용객 수`) AS `총 이용객 수`
      FROM feat_05
      WHERE YEAR(`날짜`) = {year2}
      GROUP BY MONTH(`날짜`), `역명`
    ) prev
    ON cur.`월별` = prev.`월별`
    AND cur.`역명` = prev.`역명`
    AND cur.`구분타입` = prev.`구분타입`
    CROSS JOIN (
      -- 전체 평균 (참고용 KPI)
      SELECT
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

    ORDER BY cur.`역명`, cur.`월별`,
      FIELD(cur.`구분타입`, '전체', '성수기', '비성수기');
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