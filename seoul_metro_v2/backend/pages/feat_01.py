from sqlalchemy import create_engine, text
from fastapi import APIRouter, HTTPException
from settings import settings
import pandas as pd


mariadb_engine = create_engine(settings.mariadb_url, connect_args={"local_infile": 1})

router = APIRouter(prefix="/Feat_01", tags=["Feat_01"])


@router.post("/create-table")
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

@router.post("/monthly-summary")
def create_monthly_summary():
    try:
        # 1️⃣ DB → Pandas
        select_sql = """
            SELECT
                YEAR(날짜) AS 년도,
                MONTH(날짜) AS 월,
                구분,
                total
            FROM seoul_metro
        """

        df = pd.read_sql(select_sql, mariadb_engine)

        # 2️⃣ Pandas 가공 (핵심🔥)
        result_df = df.groupby(["년도", "월", "구분"])["total"].sum().unstack(fill_value=0).reset_index()

        # 컬럼명 정리
        result_df.columns.name = None
        result_df = result_df.rename(columns={
            "승차": "승차인원합계",
            "하차": "하차인원합계"
        })

        # 없는 경우 대비
        if "승차인원합계" not in result_df:
            result_df["승차인원합계"] = 0
        if "하차인원합계" not in result_df:
            result_df["하차인원합계"] = 0

        result_df = result_df[["년도", "월", "승차인원합계", "하차인원합계"]]

        # 3️⃣ DB INSERT (덮어쓰기 or append 선택)
        result_df.to_sql(
            name="metro_monthly_summary",
            con=mariadb_engine,
            if_exists="append",  # replace / append 선택
            index=False
        )

        return {"message": "Pandas 기반 월별 집계 완료"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))