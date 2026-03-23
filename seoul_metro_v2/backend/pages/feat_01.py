from sqlalchemy import create_engine, text
from fastapi import APIRouter, HTTPException
from settings import settings


mariadb_engine = create_engine(settings.mariadb_url, connect_args={"local_infile": 1})

router = APIRouter(prefix="/Feat_01", tags=["Feat_01"])


@router.post("/create_table")
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
    
@router.post("/data_processing")
def feat_01_data_processing():
    query = """
    INSERT INTO feat_01 (년도, 월, 승차인원합계, 하차인원합계)
    SELECT
        YEAR(날짜) AS 년도,
        MONTH(날짜) AS 월,

        SUM(CASE 
            WHEN 구분 = '승차' THEN
                IFNULL(`05~06`,0)+IFNULL(`06~07`,0)+IFNULL(`07~08`,0)+
                IFNULL(`08~09`,0)+IFNULL(`09~10`,0)+IFNULL(`10~11`,0)+
                IFNULL(`11~12`,0)+IFNULL(`12~13`,0)+IFNULL(`13~14`,0)+
                IFNULL(`14~15`,0)+IFNULL(`15~16`,0)+IFNULL(`16~17`,0)+
                IFNULL(`17~18`,0)+IFNULL(`18~19`,0)+IFNULL(`19~20`,0)+
                IFNULL(`20~21`,0)+IFNULL(`21~22`,0)+IFNULL(`22~23`,0)+
                IFNULL(`23~24`,0)+IFNULL(`24~`,0)
            ELSE 0
        END),

        SUM(CASE 
            WHEN 구분 = '하차' THEN
                IFNULL(`05~06`,0)+IFNULL(`06~07`,0)+IFNULL(`07~08`,0)+
                IFNULL(`08~09`,0)+IFNULL(`09~10`,0)+IFNULL(`10~11`,0)+
                IFNULL(`11~12`,0)+IFNULL(`12~13`,0)+IFNULL(`13~14`,0)+
                IFNULL(`14~15`,0)+IFNULL(`15~16`,0)+IFNULL(`16~17`,0)+
                IFNULL(`17~18`,0)+IFNULL(`18~19`,0)+IFNULL(`19~20`,0)+
                IFNULL(`20~21`,0)+IFNULL(`21~22`,0)+IFNULL(`22~23`,0)+
                IFNULL(`23~24`,0)+IFNULL(`24~`,0)
            ELSE 0
        END)

    FROM db_metro.`seoul_metro`
    GROUP BY YEAR(날짜), MONTH(날짜);
    """

    try:
        with mariadb_engine.begin() as conn:
            # 기존 데이터 제거 (중복 방지)
            conn.execute(text("truncate table feat_01"))

            # 집계 INSERT 실행
            conn.execute(text(query))

        return {"message": "feat_01 집계 데이터 생성 완료"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))