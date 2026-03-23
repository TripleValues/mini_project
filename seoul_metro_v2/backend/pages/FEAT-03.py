import pandas as pd
from sqlalchemy import create_engine
from settings import settings

def run_feat_03():
  # ――――― [ 1. DB 연결 설정 ] ――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
  mariadb_engine = create_engine(settings.mariadb_url)

  # ――――― [ 2. 원천 데이터 가져오기 ] ――――――――――――――――――――――――――――――――――――――――――――――――――――
  raw_df = pd.read_sql("SELECT * FROM `db_metro`.`seoul_metro`", mariadb_engine)


  time_cols = []