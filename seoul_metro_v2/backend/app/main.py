from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col
from fastapi import FastAPI
from sqlalchemy import create_engine, text
from settings import settings
from pages.spark_service import process_large_csv, load_to_db
from pages.seoul_data import get_seoul_data
import pandas as pd
import os
import traceback

app = FastAPI()

spark = None
# mariadb_engine = create_engine(settings.mariadb_url)
mariadb_engine = create_engine(settings.mariadb_url, connect_args={"local_infile": 1})

@app.on_event("startup")
def startup_event():
  global spark
  os.environ["HADOOP_HOME"] = settings.hadoop_path
  os.environ["PATH"] += os.pathsep + os.path.join(settings.hadoop_path, "bin")
  try:
    spark = SparkSession.builder \
      .appName("mySparkApp") \
      .master(settings.spark_url) \
      .config("spark.driver.host", settings.host_ip) \
      .config("spark.driver.bindAddress", "0.0.0.0") \
      .config("spark.driver.port", "10000") \
      .config("spark.blockManager.port", "10001") \
      .config("spark.executor.port", "10002") \
      .config("spark.network.timeout", "800s") \
      .config("spark.rpc.askTimeout", "300s") \
      .config("spark.tcp.retries", "16") \
      .config("spark.cores.max", "2") \
      .config("spark.rpc.message.maxSize", "512") \
      .config("spark.driver.maxResultSize", "2g") \
      .config("spark.shuffle.io.maxRetries", "10") \
      .config("spark.shuffle.io.retryWait", "15s") \
      .config("spark.hadoop.fs.defaultFS", "file:///") \
      .config("spark.jars.packages", "org.mariadb.jdbc:mariadb-java-client:3.5.7") \
      .getOrCreate()
    print("Spark Session Created Successfully!")
  except Exception as e:
    print(f"Failed to create Spark session: {e}")
  
@app.on_event("shutdown")
def shutdown_event():
  if spark:
    spark.stop()

def getDataFrame(file_path):
  try:
    df = pd.read_csv(file_path, encoding="utf-8", header=0, thousands=',', quotechar='"', skipinitialspace=True)
    df.columns = df.columns.str.strip()
    return df
  except Exception as e:
    return None

def save(df, table_name):
  try:
    with mariadb_engine.connect() as conn:
      conn.execute(text("TRUNCATE TABLE seoul_metro_temp"))
      conn.commit()
    # ANSI = cp949
    # UTF = utf-8
    df.to_sql(table_name, con=mariadb_engine, if_exists='append', index=False)
    return True
  except Exception as e:
    return False
  
def selectData(df, table_name):
  try:
    # properties = {
    #  "user": settings.db_user, 
    #  "password": settings.db_password, 
    #  "driver": "org.mariadb.jdbc.Driver",
    #  "char.encoding": "utf-8",
    #  "stringtype": "unspecified"
    # }
    # spDf = spark.read.jdbc(url=settings.jdbc_url, table=table_name, properties=properties)
    # spDf = spark.read.csv(settings.file_dir, header=True, inferSchema=True, encoding="utf-8")
    # spDf = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///opt/spark/data/2008.csv")

    # query = text(f"SELECT * FROM {table_name} WHERE 구분 = '승차'")
    # df = pd.read_sql(query, con=mariadb_engine)
    spDf = spark.createDataFrame(df)

    print(spDf.columns)
    print(f"데이터 개수: {spDf.count()}")

    spDf.select("날짜", "역번호", "역명", "구분").show(10, truncate=False)

    # spDf.createOrReplaceTempView(table_name)
    # return spark.sql(f"SELECT `날짜`, `역번호`, `역명`, `구분`, `05~06`, `06~07`, `07~08`, `08~09`, `09~10`, `10~11`, `11~12` FROM {table_name}")
    # selected_df = spDf.select('날짜', '역번호', '역명', '구분', '05~06', '06~07', '07~08', '08~09', '09~10', '10~11', '11~12').filter(trim(col("구분")) == "승차")
    spDf = spDf.filter(trim(col("날짜")) != "날짜")
    selected_df = spDf.filter(trim(col("구분")) == "승차")
    # print(selected_df.columns)
    count = selected_df.count()
    print(f"필터링된 데이터 개수: {count}")

    # selected_df.write.jdbc(url=settings.jdbc_url, table=table_name, mode="append", properties=properties)

    # pdf = selected_df.toPandas()
    # chunk_size = 1000
    # pdf.to_sql(table_name, con=mariadb_engine, if_exists='replace', index=False, chunksize=chunk_size, method='multi')
    # print("데이터 적재 완료!!")

    return selected_df.limit(50).toPandas().to_dict(orient="records")
  except Exception as e:
    print(f"Failed to select data: {e}")
    return None

@app.get("/")
def read_root():
  if not spark:
    return {"status": False, "error": "Spark session not initialized"}
  try:
    df = getDataFrame(settings.file_dir)
    print("데이터 프레임 생성 완료!!")
    table_name = "seoul_metro"
    save(df, table_name)
    print("데이터 적재 완료!!")
    if not df.empty:
      result = selectData(df, table_name)
      print("데이터 프레임 변환 완료!!")
      return {"status": True, "data": result}
  except Exception as e:
    traceback.print_exc()
    return {"status": False, "error": str(e)}


# ==============================
# 적재 API
# ==============================
@app.post("/load")
def load_data():
  global spark  # 전역 변수 spark 세션 참조

  if spark is None:
    return {
      "status": False, 
      "error": "Spark session is not initialized. Please wait for startup."
    }
  
  try:
    # engine = create_engine(
    #   settings.mariadb_url,
    #   connect_args={"local_infile": 1}
    # )
    
    folder_path = settings.file_dir

    # --------------------------------------------------------------
    # 자동 commit
    # --------------------------------------------------------------
    with mariadb_engine.begin() as conn:
      # ------------------------------------------------------------
      # DB의 전 데이터 삭제하는 SQL이므로 전체 데이터 적제 시에만 주석 해제
      # 일부 데이터만 적제 시 미사용
      # ------------------------------------------------------------
      # conn.execute(text("TRUNCATE TABLE db_metro.seoul_metro"))

      # ------------------------------------------------------------
      # DB에 적제한 데이터가 잘못 정제된 경우 
      # 해당 년도의 데이터를 전부 삭제 하는 SQL문
      # 다른 값을 기준으로 삭제 할려면 year과 where변수 수정
      # ------------------------------------------------------------
      # year = 2021

      # if year != 0:
      #   # 1. 쿼리문에 :year 라고 표시 (플레이스홀더)
      #   query = text("DELETE FROM db_metro.seoul_metro WHERE YEAR(`날짜`) = :year")
        
      #   # 2. execute 시점에 두 번째 인자로 값을 전달
      #   conn.execute(query, {"year": year})
        
      #   # 3. 변경 사항 확정
      #   conn.commit()

      # ------------------------------------------------------------
      # work 폴더에 존재하는 CSV파일 정재 및 적제 시작 부분
      # 여기서 '_clean'으로 끝나는 파일이 아닌 .csv 파일만 바라봄
      # ------------------------------------------------------------
      for file in os.listdir(folder_path):
        if file.endswith(".csv") and "_clean" not in file:
          file_path = os.path.join(folder_path, file)

          try:
            print(f"정제 처리중: {file}")

            # pandas의 chunk 기반 데이터 전달(하나의 CSV를 만번씩 전달 진행) 
            # spark 기반 정제(만번씩 전달 받아서 정제 진행)
            # 즉, 원본 CSV 파일의 데이터를 확인하여 
            # 만번씩 정재된 데이터를 '_clean.csv' 파일에 저장 
            # 핵심: 초기화된 spark 객체를 인자로 넘겨줍니다.
            clean_file = process_large_csv(file_path, spark, 10000)

            # DB 적재
            # 최종 정제가 완료된 '_clean.csv' 파일을 이용하여 DB에 적재
            load_to_db(conn, clean_file)

            print(f"정제 처리완료: {file}")
          except Exception as e:
            print(f"실패: {file} / {e}")

    return {"status": True, "message": "데이터 적재 완료"}

  except Exception as e:
    return {"status": False, "error": str(e)}


# ================================================
# 서울 열린데이터 광장의 메트로 라인 공공 데이터 받아오기
# ================================================
@app.post("/seoul_load")
def load_seoul_data():
  try:
    sata = get_seoul_data()

    return {"status": True, "message": f"총 {sata}건 데이터 적재 완료"}

  except Exception as e:
    return {"status": False, "error": str(e)}