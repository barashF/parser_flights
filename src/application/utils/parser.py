import re
import io
import sys
from datetime import date, time
from typing import Optional, List, Dict, Union, Tuple
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

DB_CONFIG = {
    "user": "postgres",
    "password": "secret",
    "host": "db",
    "port": "5432",
    "database": "flights_db"
}

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS flights (
    id SERIAL PRIMARY KEY,
    shr_col TEXT,
    dep_col TEXT,
    arr_col TEXT,
    f1 TEXT,
    f2 TEXT,
    f3 TEXT,
    sid TEXT,
    reg TEXT,
    dep TEXT,
    dest TEXT,
    eet TEXT,
    zona TEXT,
    typ TEXT,
    dof DATE,
    dep_time TIME,
    arr_time TIME,
    region TEXT,
    file TEXT
);
"""

def _sanitize(val) -> Optional[str]:
    if pd.isna(val):
        return None
    return str(val).strip().rstrip(")/")

def _parse_date(d) -> Optional[date]:
    if pd.isna(d):
        return None
    try:
        return pd.to_datetime(str(d), format="%y%m%d").date()
    except Exception:
        return None

def _parse_time(t) -> Optional[time]:
    if pd.isna(t):
        return None
    s = str(t).zfill(4)
    try:
        return pd.to_datetime(s, format="%H%M").time()
    except Exception:
        return None

def _write_batch(engine, batch: List[Dict]):
    if not batch:
        return
    df = pd.DataFrame(batch)
    df.columns = [c.lower() for c in df.columns]
    try:
        df.to_sql("flights", engine, if_exists="append", index=False, method="multi")
    except IntegrityError:
        raise
    except Exception as e:
        raise RuntimeError(f"DB insert error: {e}")

def _process_xlsx(engine, content: bytes, filename: str):
    buffer = io.BytesIO(content)
    xls = pd.ExcelFile(buffer)
    for sheet in xls.sheet_names:
        df = pd.read_excel(buffer, sheet_name=sheet)
        batch = []
        sid = reg = dep = dest = dof = eet = zona = typ = None
        dep_time = arr_time = None
        region = None
        shr_out = shr_in = dep_out = dep_in = arr_out = arr_in = None

        target_sheets = ["Калининград", "Тюмень", "Красноярск", "Иркутск", "Якутск"]
        is_2024_special = filename == "2024.xlsx" and sheet in target_sheets
        is_2025 = filename == "2025.xlsx"
        if not (is_2024_special or is_2025):
            continue

        for _, row in df.iterrows():
            shr_text = row.get("SHR")
            dep_text = row.get("DEP")
            arr_text = row.get("ARR")

            region = row.get("Центр ЕС ОрВД") if is_2025 else sheet

            if pd.notna(shr_text):
                m = re.search(r"([\s\S]*)\(([\s\S]*)\)", str(shr_text))
                if m:
                    shr_out, shr_in = m.group(1), m.group(2)
                    if shr_in:
                        sid_m = re.search(r"SID/(\S+)", shr_in)
                        sid = sid_m.group(1) if sid_m else None
                        reg_m = re.search(r"REG/(\S+)", shr_in)
                        reg = reg_m.group(1) if reg_m else None
                        dep_m = re.search(r"DEP/(\S+)", shr_in)
                        dep = dep_m.group(1) if dep_m else None
                        dest_m = re.search(r"DEST/(\S+)", shr_in)
                        dest = dest_m.group(1) if dest_m else None
                        dof_m = re.search(r"DOF/(\S+)", shr_in)
                        dof = dof_m.group(1) if dof_m else None
                        eet_m = re.search(r"EET/(\S+)", shr_in)
                        eet = eet_m.group(1) if eet_m else None
                        typ_m = re.search(r"TYP/(\S+)", shr_in)
                        typ = typ_m.group(1) if typ_m else None
                        zona_m = re.search(r"ZONA ([^\/]+)\/", shr_in)
                        zona = zona_m.group(1) if zona_m else None

            if pd.notna(dep_text):
                dep_str = str(dep_text)
                if is_2025:
                    m = re.search(r"-ATD\s*(\d{4})", dep_str)
                    dep_time = m.group(1) if m else None
                else:
                    m = re.search(r"([\s\S]*)\(([\s\S]*)\)", dep_str)
                    if m:
                        dep_out, dep_in = m.group(1), m.group(2)
                        if dep_in:
                            m2 = re.search(r"DEP-[\s\S]*-ZZZZ(\d{4})", dep_in)
                            dep_time = m2.group(1) if m2 else None

            if pd.notna(arr_text):
                arr_str = str(arr_text)
                if is_2025:
                    m = re.search(r"-ATA\s*(\d{4})", arr_str)
                    arr_time = m.group(1) if m else None
                else:
                    m = re.search(r"([\s\S]*)\(([\s\S]*)\)", arr_str)
                    if m:
                        arr_out, arr_in = m.group(1), m.group(2)
                        if arr_in:
                            m2 = re.search(r"ARR-[\s\S]*-[\s\S]*-ZZZZ(\d{4})", arr_in)
                            arr_time = m2.group(1) if m2 else None

            batch.append({
                "SHR_COL": shr_text if pd.notna(shr_text) else None,
                "DEP_COL": dep_text if pd.notna(dep_text) else None,
                "ARR_COL": arr_text if pd.notna(arr_text) else None,
                "F1": _sanitize(shr_out),
                "F2": _sanitize(dep_out),
                "F3": _sanitize(arr_out),
                "SID": _sanitize(sid),
                "REG": _sanitize(reg),
                "DEP": _sanitize(dep),
                "DEST": _sanitize(dest),
                "EET": _sanitize(eet),
                "ZONA": _sanitize(zona),
                "TYP": _sanitize(typ),
                "DOF": _parse_date(dof),
                "DEP_TIME": _parse_time(dep_time),
                "ARR_TIME": _parse_time(arr_time),
                "REGION": _sanitize(region),
                "FILE": filename
            })

            if len(batch) >= 100:
                _write_batch(engine, batch)
                batch = []

        if batch:
            _write_batch(engine, batch)

def _process_csv(engine, content: bytes, filename: str):
    df = pd.read_csv(io.BytesIO(content), dtype=str)
    batch = []
    sid = reg = dep = dest = dof = eet = zona = typ = None
    dep_time = arr_time = None
    region = None
    shr_out = shr_in = dep_out = dep_in = arr_out = arr_in = None

    is_2025 = filename == "2025.csv"
    is_2024 = filename == "2024.csv"

    if not (is_2024 or is_2025):
        return

    for _, row in df.iterrows():
        shr_text = row.get("SHR")
        dep_text = row.get("DEP")
        arr_text = row.get("ARR")

        region = row.get("Центр ЕС ОрВД") if is_2025 else "CSV"

        if pd.notna(shr_text):
            m = re.search(r"([\s\S]*)\(([\s\S]*)\)", str(shr_text))
            if m:
                shr_out, shr_in = m.group(1), m.group(2)
                if shr_in:
                    sid_m = re.search(r"SID/(\S+)", shr_in)
                    sid = sid_m.group(1) if sid_m else None
                    reg_m = re.search(r"REG/(\S+)", shr_in)
                    reg = reg_m.group(1) if reg_m else None
                    dep_m = re.search(r"DEP/(\S+)", shr_in)
                    dep = dep_m.group(1) if dep_m else None
                    dest_m = re.search(r"DEST/(\S+)", shr_in)
                    dest = dest_m.group(1) if dest_m else None
                    dof_m = re.search(r"DOF/(\S+)", shr_in)
                    dof = dof_m.group(1) if dof_m else None
                    eet_m = re.search(r"EET/(\S+)", shr_in)
                    eet = eet_m.group(1) if eet_m else None
                    typ_m = re.search(r"TYP/(\S+)", shr_in)
                    typ = typ_m.group(1) if typ_m else None
                    zona_m = re.search(r"ZONA ([^\/]+)\/", shr_in)
                    zona = zona_m.group(1) if zona_m else None

        if pd.notna(dep_text):
            dep_str = str(dep_text)
            if is_2025:
                m = re.search(r"-ATD\s*(\d{4})", dep_str)
                dep_time = m.group(1) if m else None
            else:
                m = re.search(r"([\s\S]*)\(([\s\S]*)\)", dep_str)
                if m:
                    dep_out, dep_in = m.group(1), m.group(2)
                    if dep_in:
                        m2 = re.search(r"DEP-[\s\S]*-ZZZZ(\d{4})", dep_in)
                        dep_time = m2.group(1) if m2 else None

        if pd.notna(arr_text):
            arr_str = str(arr_text)
            if is_2025:
                m = re.search(r"-ATA\s*(\d{4})", arr_str)
                arr_time = m.group(1) if m else None
            else:
                m = re.search(r"([\s\S]*)\(([\s\S]*)\)", arr_str)
                if m:
                    arr_out, arr_in = m.group(1), m.group(2)
                    if arr_in:
                        m2 = re.search(r"ARR-[\s\S]*-[\s\S]*-ZZZZ(\d{4})", arr_in)
                        arr_time = m2.group(1) if m2 else None

        batch.append({
            "SHR_COL": shr_text if pd.notna(shr_text) else None,
            "DEP_COL": dep_text if pd.notna(dep_text) else None,
            "ARR_COL": arr_text if pd.notna(arr_text) else None,
            "F1": _sanitize(shr_out),
            "F2": _sanitize(dep_out),
            "F3": _sanitize(arr_out),
            "SID": _sanitize(sid),
            "REG": _sanitize(reg),
            "DEP": _sanitize(dep),
            "DEST": _sanitize(dest),
            "EET": _sanitize(eet),
            "ZONA": _sanitize(zona),
            "TYP": _sanitize(typ),
            "DOF": _parse_date(dof),
            "DEP_TIME": _parse_time(dep_time),
            "ARR_TIME": _parse_time(arr_time),
            "REGION": _sanitize(region),
            "FILE": filename
        })

        if len(batch) >= 100:
            _write_batch(engine, batch)
            batch = []

    if batch:
        _write_batch(engine, batch)

def parse_file(filename: str, content: bytes) -> Optional[str]:
    url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(url)
    with engine.connect() as conn:
        conn.execute(text(TABLE_DDL))
        conn.commit()

    try:
        if filename.lower().endswith('.xlsx'):
            _process_xlsx(engine, content, filename)
        elif filename.lower().endswith('.csv'):
            _process_csv(engine, content, filename)
        else:
            return "format only xlsx csv"
        return None
    except IntegrityError:
        return "nique constraint violation"
    except Exception as e:
        return f"Processing failed: {str(e)}"