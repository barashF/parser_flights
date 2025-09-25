import re
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import io


POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "secret"
POSTGRES_HOST = "db"
POSTGRES_PORT = "5432"
POSTGRES_DB = "flights_db"


TABLE_CREATION_QUERY = """
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


def parse_date_string(date_str):
    if pd.isna(date_str):
        return None
    try:
        return pd.to_datetime(str(date_str), format="%y%m%d").date()
    except Exception:
        return None


def normalize_time_string(time_str):
    if pd.isna(time_str):
        return None
    normalized = str(time_str).zfill(4)
    try:
        return pd.to_datetime(normalized, format="%H%M").time()
    except Exception:
        return None


def sanitize_string(value):
    if pd.isna(value):
        return None
    cleaned = str(value).strip().rstrip(")/")
    return cleaned if cleaned else None


def save_records_to_database(record_batch):
    try:
        df = pd.DataFrame(record_batch)
        df.columns = df.columns.str.lower()
        df.to_sql("flights", engine, if_exists="append", index=False, method="multi")
    except IntegrityError as e:
        print(str(e))
        sys.exit(1)
    except Exception as e:
        print(str(e))
        sys.exit(1)


def parse_csv_file(file_bytes: bytes, filename: str):
    region = filename.rsplit('.', 1)[0]  

    df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, encoding="utf-8")

    batch_records = []

    for _, row in df.iterrows():
        shr_text = row.get("SHR")
        dep_text = row.get("DEP")
        arr_text = row.get("ARR")

        sid = reg = dep_airport = dest_airport = dof = eet = zona = typ = None
        dep_time = arr_time = None
        shr_outside_parenth = shr_inside_parenth = None
        dep_outside_parenth = dep_inside_parenth = None
        arr_outside_parenth = arr_inside_parenth = None

        if pd.notna(shr_text):
            shr_match = re.search(r"([\s\S]*)\(([\s\S]*)\)", str(shr_text))
            if shr_match:
                shr_outside_parenth = shr_match.group(1).strip() or None
                shr_inside_parenth = shr_match.group(2).strip() or None

                if shr_inside_parenth:
                    fields = parse_shr_text(shr_text)
                    sid = fields.get("SID")
                    reg = fields.get("REG")
                    dep_airport = fields.get("DEP")
                    dest_airport = fields.get("DEST")
                    dof = fields.get("DOF")
                    eet = fields.get("EET")
                    typ = fields.get("TYP")

                    zona_match = re.search(r"ZONA\s*([^A-Z/]*(?:[A-Z](?![A-Z]*/)[^A-Z/]*)*)", shr_inside_parenth)
                    zona = zona_match.group(1).strip() if zona_match else None

        if pd.notna(dep_text):
            dep_time_match = re.search(r"-ATD\s*(\d{4})", str(dep_text))
            if not dep_time_match:
                dep_time_match = re.search(r"-ZZZZ\s*(\d{4})", str(dep_text))
            dep_time = dep_time_match.group(1) if dep_time_match else None

    
        if pd.notna(arr_text):
            arr_time_match = re.search(r"-ATA\s*(\d{4})", str(arr_text))
            if not arr_time_match:
                arr_time_match = re.search(r"-ZZZZ\s*(\d{4})", str(arr_text))
            arr_time = arr_time_match.group(1) if arr_time_match else None

      
        record = {
            "shr_col": sanitize_string(shr_text),
            "dep_col": sanitize_string(dep_text),
            "arr_col": sanitize_string(arr_text),
            "f1": sanitize_string(shr_outside_parenth),
            "f2": sanitize_string(dep_outside_parenth),
            "f3": sanitize_string(arr_outside_parenth),
            "sid": sanitize_string(sid),
            "reg": sanitize_string(reg),
            "dep": sanitize_string(dep_airport),
            "dest": sanitize_string(dest_airport),
            "eet": sanitize_string(eet),
            "zona": sanitize_string(zona),
            "typ": sanitize_string(typ),
            "dof": parse_date_string(dof),
            "dep_time": normalize_time_string(dep_time),
            "arr_time": normalize_time_string(arr_time),
            "region": sanitize_string(region),
            "file": filename
        }

        batch_records.append(record)

        if len(batch_records) >= 100:
            save_records_to_database(batch_records)
            batch_records = []

    if batch_records:
        save_records_to_database(batch_records)


def process_excel_file(file_bytes: bytes, filename: str):
    excel_data = pd.ExcelFile(io.BytesIO(file_bytes))
    for sheet in excel_data.sheet_names:
        df = excel_data.parse(sheet)
        batch_records = []

        flight_sid = aircraft_reg = departure_airport = destination_airport = None
        flight_date = estimated_time_enroute = operational_zone = aircraft_type = None
        departure_time = arrival_time = None
        operational_region = "Центр ЕС ОрВД"

        for _, row in df.iterrows():
            shr_cell = row.get("SHR")
            dep_cell = row.get("DEP")
            arr_cell = row.get("ARR")

            if pd.notna(shr_cell):
                shr_pattern = re.search(r"([\s\S]*)\(([\s\S]*)\)", str(shr_cell))
                if shr_pattern:
                    shr_prefix = shr_pattern.group(1).strip() if shr_pattern.group(1) else None
                    shr_details = shr_pattern.group(2).strip() if shr_pattern.group(2) else None

                    if shr_details:
                        flight_sid = re.search(r"SID/(\S+)", shr_details)
                        aircraft_reg = re.search(r"REG/([^\/]+)", shr_details)
                        departure_airport = re.search(r"DEP/(\S+)", shr_details)
                        destination_airport = re.search(r"DEST/(\S+)", shr_details)
                        flight_date = re.search(r"DOF/(\S+)", shr_details)
                        estimated_time_enroute = re.search(r"EET/(\S+)", shr_details)
                        aircraft_type = re.search(r"TYP/([^\/]+)", shr_details)
                        operational_zone = re.search(r"ZONA ([\s\S][^\/]+)\/", shr_details)

                        flight_sid = flight_sid.group(1) if flight_sid else None
                        aircraft_reg = aircraft_reg.group(1) if aircraft_reg else None
                        departure_airport = departure_airport.group(1) if departure_airport else None
                        destination_airport = destination_airport.group(1) if destination_airport else None
                        flight_date = flight_date.group(1) if flight_date else None
                        estimated_time_enroute = estimated_time_enroute.group(1) if estimated_time_enroute else None
                        aircraft_type = aircraft_type.group(1) if aircraft_type else None
                        operational_zone = operational_zone.group(1) if operational_zone else None

            if pd.notna(dep_cell):
                dep_time_match = re.search(r"-ATD[\s]*(\d{4})", str(dep_cell))
                departure_time = dep_time_match.group(1) if dep_time_match else None

            if pd.notna(arr_cell):
                arr_time_match = re.search(r"-ATA[\s]*(\d{4})", str(arr_cell))
                arrival_time = arr_time_match.group(1) if arr_time_match else None

            record = {
                "shr_col": sanitize_string(shr_cell),
                "dep_col": sanitize_string(dep_cell),
                "arr_col": sanitize_string(arr_cell),
                "f1": sanitize_string(shr_prefix),
                "f2": sanitize_string(dep_prefix),
                "f3": sanitize_string(arr_prefix),
                "sid": sanitize_string(flight_sid),
                "reg": sanitize_string(aircraft_reg),
                "dep": sanitize_string(departure_airport),
                "dest": sanitize_string(destination_airport),
                "eet": sanitize_string(estimated_time_enroute),
                "zona": sanitize_string(operational_zone),
                "typ": sanitize_string(aircraft_type),
                "dof": parse_date_string(flight_date),
                "dep_time": normalize_time_string(departure_time),
                "arr_time": normalize_time_string(arrival_time),
                "region": sanitize_string(operational_region),
                "file": filename
            }
            batch_records.append(record)

            if len(batch_records) >= 100:
                save_records_to_database(batch_records)
                batch_records = []

        if batch_records:
            save_records_to_database(batch_records)


def parse_file(file_bytes: bytes, filename: str):
    global engine
    engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    with engine.connect() as connection:
        try:
            connection.execute(text(TABLE_CREATION_QUERY))
            connection.commit()
        except Exception as e:
            print(str(e))
            sys.exit(1)

    if filename.lower().endswith(".xlsx"):
        process_excel_file(file_bytes, filename)
    elif filename.lower().endswith(".csv"):
        parse_csv_file(file_bytes, filename)
    else:
        raise ValueError("формат дерьма, принимаю только .xlsx и .csd")