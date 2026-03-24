from fastapi import APIRouter
from pyspark.sql import functions as F
from settings import settings

router = APIRouter(prefix="/Feat_03", tags=["Feat_03"])

def get_time_pattern(spark, year: str, station_name: str, day_type: str):
  # ――――― [ DB 연결 및 데이터 로드 ] ――――――――――――――――――――――――――――――――――――――――――――――――――
  df = spark.read.format("jdbc") \
  .option("url", settings.jdbc_url) \
  .option("driver", "org.mariadb.jdbc.Driver") \
  .option("dbtable", "db_metro.feat_03") \
  .option("user", settings.mariadb_user) \
  .option("password", settings.mariadb_password) \
  .load()

  # ===================================================================================
  # 1. MySQL Connection 방법 - 1
  # ===================================================================================

  # ――――― [ 데이터 정제 및 필터링 시도 >>> 실패 ] ――――――――――――――――――――――――――――――――――――――――――――――――――――――
  # - split: '서울역(150)'에서 '('를 기준으로 나누어 앞부분만 취함
  # - trim: 혹시 모를 앞뒤 공백 제거
  # - cast: DB의 DECIMAL 타입을 float으로 변환 (JSON 직렬화 에러 방지)
  
  # target_year = int(year)
  # search_station = f"{station_name.strip()}%"

  # sdf = df.filter(
  #   (F.col("연도") == target_year) & 
  #   (F.col("역명").like(search_station)) & 
  #   (F.col("요일구분").contains(day_type.strip()))
  # )

  # ――――― [ 데이터 정제 및 필터링 시도 >>> 성공 ] ――――――――――――――――――――――――――――――――――――――――――――――――――――――
  refined_sdf = df.withColumn(
    "정제역명", 
    F.trim(F.substring_index(F.col("역명"), "(", 1 ))
  ).filter(
      (F.col("연도") == int(year)) &
      (F.col("정제역명") == station_name.strip()) &
      (F.col("요일구분") == day_type.strip())
    )
  
  # ――――― [ 필요한 컬럼 선택 및 정렬 ] ―――――――――――――――――――――――――――――――――――――――――――――――――――
  result_sdf = refined_sdf.select(
    F.col("시간대"),
    F.col("역번호"),
    F.col("구분"),
    F.col("평균인원").cast("float"),
    F.col("혼잡도지수").cast("float")
  ).orderBy("시간대")

  # ――――― [ Pandas 변환 & 결과 return ] ――――――――――――――――――――――――――――――――――――――――――――――
  pdf = result_sdf.toPandas()
  if pdf.empty:
    print(f"데이터 없음: {year}, {station_name}, {day_type}")
    return[]
  return pdf.to_dict(orient="records")

  # ===================================================================================
  # 2. MySQL Connection 방법 - 2 >>> return이 빈 배열만 나옴
  # ===================================================================================
  # ――――― [ SQL 사용을 위한 임시 뷰 생성 ] ―――――――――――――――――――――――――――――――――――――――――――――――
  # print(f"✅ 전체 로드된 데이터 건수: {df.count()}")
  # df.createOrReplaceTempView("feat_03_view")

  # # ――――― [ SQL 쿼리 작성 ] ――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
  # safe_station = station_name.replace("'", "''")
  # sql = f"""
  #   select `시간대`, `구분`, `평균인원`, `혼잡도지수`
  #   from (
  #     select *, 
  #       trim(substring_index(`역명`, '(', 1)) as `정제역명` 
  #     from feat_03_view
  #   ) as t
  #   where `연도` = {int(year)}
  #     and `정제역명` = '{safe_station}'
  #     and `요일구분` = '{day_type}'
  #   order by `시간대`
  # """
  # # ――――― [ 쿼리 실행 및 Pandas 변환 ] ―――――――――――――――――――――――――――――――――――――――――――――――――――
  # pdf = spark.sql(sql).toPandas()
  
  # # pdf['평균인원'] = pdf['평균인원'].astype(float)
  # # pdf['혼잡도지수'] = pdf['혼잡도지수'].astype(float)

  # if pdf.empty:
  #   return []

  # return pdf.to_dict(orient="records")

@router.get("/metro_03", tags=["Feat_03"])
async def read_time_pattern(year: str, station_name: str, day_type: str):
  from main import spark

  # ――――― [ Spark 세션 체크 ] ――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
  if not spark:
    return {"status" : False, "message": "Spark 세션 초기화 실패"}
  
  try:
    # ――――― [ 'feat_03.py' 호출 ] ――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    data = get_time_pattern(spark, year, station_name, day_type)
    return {
      "status" : True,
      "metadata" : {"year": year, "station": station_name, "day_type": day_type},
      "result": data
    }
  except Exception as e:
    return {"status": False, "error": str(e)}