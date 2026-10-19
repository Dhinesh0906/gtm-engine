"""Read-only data viewer server — FastAPI, GET-only, separate port."""

from __future__ import annotations

import csv
import io
import sys
from typing import Any

from fastapi import FastAPI, Depends, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Session

from app.config import settings
from app.viewer.db import get_db
from app.viewer import queries as q

import os as _os

app = FastAPI(title="GTM Engine Data Viewer")

_tpl_dir = _os.path.join(_os.path.dirname(__file__), "templates")
_static_dir = _os.path.join(_os.path.dirname(__file__), "static")
templates = Jinja2Templates(directory=_tpl_dir)
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


# -------------------------------------------------------------------
# Pages
# -------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
def home(request: Request, db: Session = Depends(get_db)):
    s = q.summary(db)
    return templates.TemplateResponse("home.html", {"request": request, "summary": s})


@app.get("/data", response_class=HTMLResponse)
def browse_data(request: Request, db: Session = Depends(get_db),
              industry: str = "", country: str = "", has_email: int = 0,
              q_search: str = "", page: int = 1):
    d = q.data_page(db, industry=industry, country=country, has_email=bool(has_email), q=q_search, page=page,
                    page_size=settings.viewer_page_size)
    return templates.TemplateResponse("data.html", {
        "request": request, "data": d,
        "filters": {"industry": industry, "country": country, "has_email": has_email, "q": q},
    })


@app.get("/company/{company_id}", response_class=HTMLResponse)
def company_page(company_id: int, request: Request, db: Session = Depends(get_db)):
    detail = q.company_detail(company_id, db)
    if not detail:
        return HTMLResponse("Not found", status_code=404)
    return templates.TemplateResponse("company.html", {"request": request, "detail": detail})


@app.get("/export.csv")
def export_csv(request: Request, db: Session = Depends(get_db),
               industry: str = "", country: str = "", has_email: int = 0, q: str = ""):
    d = q.data_page(db, industry=industry, country=country, has_email=bool(has_email), q=q, page=1,
                    page_size=settings.viewer_export_max)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["company_id", "name", "email", "phone", "description", "address", "industry", "country"])
    for row in d["rows"]:
        writer.writerow([row["company_id"], row["name"], row["email"], row["phone"],
                         row["description"], row["address"], row["industry"], row["country"]])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=gtm_engine_export.csv"})


# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------


def main():
    import uvicorn
    uvicorn.run(app, host=settings.viewer_host, port=settings.viewer_port, log_level="info")


if __name__ == "__main__":
    main()
