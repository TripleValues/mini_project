import pandas as pd
import requests
from sqlalchemy import create_engine, text
from settings import settings

def fetch_data():
  all_data = []

  for start in range(1, 5000, 1000):
    end = start + 999
    url = f"http://openapi.seoul.go.kr:8088/{settings.api_key}/json/SearchSTNBySubwayLineInfo/{start}/{end}"
    
    res = requests.get(url).json()

    try:
      rows = res['SearchSTNBySubwayLineInfo']['row']
      all_data.extend(rows)
    except:
      break

  df = pd.DataFrame(all_data)

  # 컬럼명 DB 맞춤 변환
  df = df.rename(columns={
    'STATION_CD': '역번호',
    'STATION_NM': '역명',
    'LINE_NUM': '호선',
    'FR_CODE': '외부코드'
  })

  # 필요한 컬럼만 (순서 중요)
  df = df[
    [
      '역번호',
      '역명',
      '호선',
      '외부코드'
    ]
  ]

  # 공백 처리
  df = df.replace('', None)

  return df


def get_seoul_data():
  df = fetch_data()
  mariadb_engine = create_engine(settings.mariadb_url)

  with mariadb_engine.connect() as conn:
    # 기존 데이터 삭제
    conn.execute(text("TRUNCATE TABLE metro_line"))

    # 데이터 적재
    df.to_sql(
      name='metro_line',
      con=conn,
      if_exists='append',
      index=False
    )
    conn.commit()

  print(f"총 {len(df)}건 적재 완료")

  return len(df)