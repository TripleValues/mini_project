from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text
import pandas as pd
import io

router = APIRouter()

@router.get("/metro")
def get_metro_data(
    type: str = Query("top50"),   # top50 / export
    date: str = Query(None),
    time: str = Query(None),
    category: str = Query(None),  # 승차 / 하차 / 총
    format: str = Query("json")   # json / csv / xlsx
):
    try:
        # ✅ 기본 쿼리
        query = """
        SELECT 날짜, 역명, 시간대, 기준, 인원, 순위
        FROM feat_02
        WHERE 1=1
        """

        params = {}

        # ✅ 필터 조건
        if date:
            query += " AND 날짜 = :date"
            params["date"] = date

        if time:
            query += " AND 시간대 = :time"
            params["time"] = time

        if category:
            query += " AND 기준 = :category"
            params["category"] = category

        query += " ORDER BY 순위 ASC"

        # ✅ DB 조회 → pandas
        df = pd.read_sql(text(query), mariadb_engine, params=params)

        # =========================
        # 🔥 JSON 응답 (기본)
        # =========================
        if format == "json":
            return df.to_dict(orient="records")

        # =========================
        # 🔥 CSV 다운로드
        # =========================
        elif format == "csv":
            stream = io.StringIO()
            df.to_csv(stream, index=False, encoding="utf-8-sig")

            return StreamingResponse(
                iter([stream.getvalue()]),
                media_type="text/csv",
                headers={
                    "Content-Disposition": "attachment; filename=metro_data.csv"
                }
            )

        # =========================
        # 🔥 XLSX 다운로드
        # =========================
        elif format == "xlsx":
            stream = io.BytesIO()

            with pd.ExcelWriter(stream, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="data")

            stream.seek(0)

            return StreamingResponse(
                stream,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": "attachment; filename=metro_data.xlsx"
                }
            )

        else:
            raise HTTPException(status_code=400, detail="Invalid format")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))