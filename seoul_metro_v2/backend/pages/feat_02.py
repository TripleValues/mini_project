from sqlalchemy import create_engine, text
from fastapi import APIRouter, HTTPException
from settings import settings

mariadb_engine = create_engine(settings.mariadb_url, connect_args={"local_infile": 1})

router = APIRouter(prefix="/Feat_02", tags=["Feat_02"])

@router.post("/create-table")
def create_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS db_metro.feat_02 (
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