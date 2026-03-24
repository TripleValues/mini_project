from settings import settings


# mariadb 데이터 가저오기 위한 spark 읽기 부분
def get_mariadb_df(spark, query: str):
  return spark.read.format("jdbc") \
    .option("url", f"{settings.jdbc_url}") \
    .option("dbtable", f"({query}) AS tmp") \
    .option("user", f"{settings.mariadb_user}") \
    .option("password", f"{settings.mariadb_password}") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .load()

# limit를 주어 스트리밍 방식으로 사용하도록 변경
# collect 안쓰음...?! 마마무(아마두)
def df_to_json_safe(df, limit):
  return [row.asDict() for row in df.limit(limit).toLocalIterator()]


# 계절 DATA를 가져오기 위한 부분
def get_seacon_data(spark, years:int, season:str): 
  query = f"""
    SELECT
      sq.`날짜`, d.`요일`, sq.`구분`,
      SUM(sq.`05~06`) AS `05~06`, SUM(sq.`06~07`) AS `06~07`, 
      SUM(sq.`07~08`) AS `07~08`, SUM(sq.`08~09`) AS `08~09`, 
      SUM(sq.`09~10`) AS `09~10`, SUM(sq.`10~11`) AS `10~11`, 
      SUM(sq.`11~12`) AS `11~12`, SUM(sq.`12~13`) AS `12~13`, 
      SUM(sq.`13~14`) AS `13~14`, SUM(sq.`14~15`) AS `14~15`, 
      SUM(sq.`15~16`) AS `15~16`, SUM(sq.`16~17`) AS `16~17`, 
      SUM(sq.`17~18`) AS `17~18`, SUM(sq.`18~19`) AS `18~19`, 
      SUM(sq.`19~20`) AS `19~20`, SUM(sq.`20~21`) AS `20~21`, 
      SUM(sq.`21~22`) AS `21~22`, SUM(sq.`22~23`) AS `22~23`, 
      SUM(sq.`23~24`) AS `23~24`, SUM(sq.`24~`) AS `24~`
    FROM `feat_05` AS sq
    LEFT JOIN `요일` AS d
      ON sq.`요일` = d.`코드`
    LEFT JOIN `metro_line` AS ml
      ON sq.`역번호` = ml.`역번호`
    WHERE YEAR(`날짜`) = {years}
      AND sq.`계절` = '{season}'
    GROUP BY sq.`날짜`, d.`요일`, sq.`구분`
  """
  
  outputData = get_mariadb_df(spark, query)
  
  return outputData