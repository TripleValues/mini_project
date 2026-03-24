from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, text
from settings import settings
import pandas as pd
import io

mariadb_engine = create_engine(settings.mariadb_url, connect_args={"local_infile": 1})

router = APIRouter()

@router.get("/export")
def export_data(
    feat: str = Query(..., description="feat_01 ~ feat_05"),
    format: str = Query("csv", description="csv | json | xlsx")
):
    try:
        # =========================
        # 🔥 허용 테이블 제한 (보안)
        # =========================
        allowed_tables = ["feat_01", "feat_02", "feat_03", "feat_04", "feat_05"]

        if feat not in allowed_tables:
            raise HTTPException(status_code=400, detail="잘못된 feat 값")

        # =========================
        # 🔥 데이터 조회
        # =========================
        query = f"SELECT * FROM {feat}"

        df = pd.read_sql(text(query), mariadb_engine)

        if df.empty:
            raise HTTPException(status_code=404, detail="데이터 없음")

        # =========================
        # 📁 CSV 다운로드
        # =========================
        if format == "csv":
            stream = io.StringIO()
            df.to_csv(stream, index=False)
            stream.seek(0)

            return StreamingResponse(
                iter([stream.getvalue()]),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={feat}.csv"
                }
            )

        # =========================
        # 📁 JSON 다운로드
        # =========================
        elif format == "json":
            stream = io.StringIO()
            df.to_json(stream, orient="records", force_ascii=False)
            stream.seek(0)

            return StreamingResponse(
                iter([stream.getvalue()]),
                media_type="application/json",
                headers={
                    "Content-Disposition": f"attachment; filename={feat}.json"
                }
            )

        # =========================
        # 📁 XLSX 다운로드
        # =========================
        elif format == "xlsx":
            stream = io.BytesIO()
            with pd.ExcelWriter(stream, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name=feat)

            stream.seek(0)

            return StreamingResponse(
                stream,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename={feat}.xlsx"
                }
            )

        else:
            raise HTTPException(status_code=400, detail="지원하지 않는 format")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))