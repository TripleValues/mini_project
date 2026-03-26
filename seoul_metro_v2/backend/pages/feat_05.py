from fastapi import APIRouter
from settings import settings
from pydantic import BaseModel
import time
import logging

router = APIRouter(prefix="/Feat_05", tags=["Feat_05"])

logger = logging.getLogger("uvicorn")

# ==================================================================
# DB LOAD
# mariadb 데이터 가저오기 위한 spark 읽기 부분
# ==================================================================
def get_mariadb_df(spark, query: str):
  return spark.read.format("jdbc") \
    .option("url", f"{settings.jdbc_url}") \
    .option("dbtable", f"({query}) AS tmp") \
    .option("user", f"{settings.mariadb_user}") \
    .option("password", f"{settings.mariadb_password}") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .load()


# ===================================
# REQUEST MODEL
# ===================================
class SeasonalityRequest(BaseModel):
    year1: int
    year2: int
    month: int | None = None
    type: str = "전체"


# ============================================================
# feat-05 특정연도와 특정연도의 월별 총 이용객 증감률 데이터 가져오기
# ============================================================

@router.post("/metro_05_1")
async def get_gr_metro_data(req: SeasonalityRequest):
  from main import spark

  if spark is None:
    return {"status": False, "error": "Spark session not initialized"}

  try:
    year1 = req.year1
    year2 = req.year2
    month = req.month
    type_ = req.type

    logger.info("[DB 조회 시작]")

    # 🔥 공통 필터
    type_filter = f"AND 성수기구분 = '{type_}'" if type_ != "전체" else ""

    # ---------------------------------------------------------------------
    # 1. 월별 (month 없음)
    # ---------------------------------------------------------------------
    if month is None:

      query = f"""
        SELECT
          `월별`,
          SUM(`총 이용객 수`) AS `총 이용객 수`
        FROM feat_05
        WHERE `연도` = {year1}
        {type_filter}
        GROUP BY `월별`
      """

      df = get_mariadb_df(spark, query)
      data = df.toPandas().to_dict(orient="records")

      # 증감률 계산 (Python에서)
      prev_query = f"""
        SELECT
          `월별`,
          SUM(`총 이용객 수`) AS `총 이용객 수`
        FROM feat_05
        WHERE `연도` = {year2}
        {type_filter}
        GROUP BY `월별`
      """

      prev_df = get_mariadb_df(spark, prev_query)
      prev_data = {d["월별"]: d["총 이용객 수"] for d in prev_df.toPandas().to_dict(orient="records")}

      result = []
      for d in data:
        m = d["월별"]
        cur_val = d["총 이용객 수"]
        prev_val = prev_data.get(m, 0)

        growth = 0
        if prev_val != 0:
          growth = (cur_val - prev_val) / prev_val * 100

        result.append({
          "월별": m,
          "총_이용객_증감률": round(growth, 2)
        })

      return {"status": True, "monthly": result}


    # ---------------------------------------------------------------------
    # 2. 역별 (month 있음)
    # ---------------------------------------------------------------------
    else:

      query = f"""
        SELECT
          `역명`,
          SUM(`총 이용객 수`) AS `총 이용객 수`
        FROM feat_05
        WHERE `연도` = {year1}
        AND `월별` = {month}
        {type_filter}
        GROUP BY `역명`
      """

      df = get_mariadb_df(spark, query)
      data = df.toPandas().to_dict(orient="records")

      prev_query = f"""
        SELECT
          `역명`,
          SUM(`총 이용객 수`) AS `총 이용객 수`
        FROM feat_05
        WHERE `연도` = {year2}
        AND `월별` = {month}
        {type_filter}
        GROUP BY `역명`
      """

      prev_df = get_mariadb_df(spark, prev_query)
      prev_data = {d["역명"]: d["총 이용객 수"] for d in prev_df.toPandas().to_dict(orient="records")}

      result = []
      for d in data:
        name = d["역명"]
        cur_val = d["총 이용객 수"]
        prev_val = prev_data.get(name, 0)

        growth = 0
        if prev_val != 0:
          growth = (cur_val - prev_val) / prev_val * 100

        result.append({
          "역명": name,
          "총_이용객_증감률": round(growth, 2)
        })

      return {"status": True, "stations": result}


  except Exception as e:
    logger.error(str(e))
    return {"status": False, "error": str(e)}