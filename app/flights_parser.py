import re
import sys
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "secret"
POSTGRES_HOST = "db"
POSTGRES_PORT = "5432"
POSTGRES_DB = "flights_db"


TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS flights (
    id SERIAL PRIMARY KEY,
    shr_original TEXT,
    dep_original TEXT,
    arr_original TEXT,
    shr_prefix TEXT,
    dep_prefix TEXT,
    arr_prefix TEXT,
    flight_sid TEXT,
    aircraft_reg TEXT,
    departure_airport TEXT,
    destination_airport TEXT,
    estimated_time_enroute TEXT,
    operational_zone TEXT,
    aircraft_type TEXT,
    flight_date DATE,
    departure_time TIME,
    arrival_time TIME,
    region TEXT,
    source_file TEXT
);
"""


def parse_date_string(date_str):
    # Преобразует строку даты в формате YYMMDD в объект date.
    # Возвращает None, если строка пустая или некорректная.
    if pd.isna(date_str):
        return None
    try:
        return pd.to_datetime(str(date_str), format="%y%m%d").date()
    except Exception:
        return None


def normalize_time_string(time_str):
    # Нормализует строку времени в формате HHMM.
    # Добавляет ведущие нули при необходимости и преобразует в объект time.
    # Возвращает None, если строка пустая или некорректная.
    if pd.isna(time_str):
        return None
    normalized = str(time_str).zfill(4)
    try:
        return pd.to_datetime(normalized, format="%H%M").time()
    except Exception:
        return None


def sanitize_string(value):
    # Нормализует строку времени в формате HHMM.
    # Добавляет ведущие нули при необходимости и преобразует в объект time.
    # Возвращает None, если строка пустая или некорректная.
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


def process_excel_file(file_path):
    excel_data = pd.ExcelFile(file_path)
    for sheet in excel_data.sheet_names:
        print(f"Обработка листа '{sheet}' в файле {file_path.name}")
        df = excel_data.parse(sheet)
        batch_records = []
        row_counter = 0

        # Инициализация переменных для хранения извлеченных данных
        flight_sid = aircraft_reg = departure_airport = destination_airport = None
        flight_date = estimated_time_enroute = operational_zone = aircraft_type = None
        departure_time = arrival_time = None
        operational_region = None

        # Определение региона для разных файлов
        if file_path.name == "2025.xlsx":
            operational_region = "Центр ЕС ОрВД"  
        elif file_path.name == "2024.xlsx":
            operational_region = sheet  

        # Обработка каждой строки в листе
        for _, row in df.iterrows():
            row_counter += 1

            shr_cell = row.get("SHR")
            dep_cell = row.get("DEP")
            arr_cell = row.get("ARR")

            # Обработка столбца SHR (Flight Route)
            if pd.notna(shr_cell):
                shr_pattern = re.search(r"([\s\S]*)\(([\s\S]*)\)", str(shr_cell))
                if shr_pattern:
                    shr_prefix = shr_pattern.group(1).strip() if shr_pattern.group(1) else None
                    shr_details = shr_pattern.group(2).strip() if shr_pattern.group(2) else None

                    # Извлечение деталей из SHR
                    if shr_details:
                        flight_sid = re.search(r"SID/(\S+)", shr_details)
                        aircraft_reg = re.search(r"REG/([^\/]+)", shr_details)
                        departure_airport = re.search(r"DEP/(\S+)", shr_details)
                        destination_airport = re.search(r"DEST/(\S+)", shr_details)
                        flight_date = re.search(r"DOF/(\S+)", shr_details)
                        estimated_time_enroute = re.search(r"EET/(\S+)", shr_details)
                        aircraft_type = re.search(r"TYP/([^\/]+)", shr_details)
                        operational_zone = re.search(r"ZONA ([\s\S][^\/]+)\/", shr_details)

                        # Приведение извлеченных значений к нужному формату
                        flight_sid = flight_sid.group(1) if flight_sid else None
                        aircraft_reg = aircraft_reg.group(1) if aircraft_reg else None
                        departure_airport = departure_airport.group(1) if departure_airport else None
                        destination_airport = destination_airport.group(1) if destination_airport else None
                        flight_date = flight_date.group(1) if flight_date else None
                        estimated_time_enroute = estimated_time_enroute.group(1) if estimated_time_enroute else None
                        aircraft_type = aircraft_type.group(1) if aircraft_type else None
                        operational_zone = operational_zone.group(1) if operational_zone else None

            # Обработка столбца DEP (Departure)
            if pd.notna(dep_cell):
                if file_path.name == "2025.xlsx":
                    dep_time_match = re.search(r"-ATD[\s]*(\d{4})", str(dep_cell))
                    departure_time = dep_time_match.group(1) if dep_time_match else None
                elif file_path.name == "2024.xlsx":
                    dep_pattern = re.search(r"([\s\S]*)\(([\s\S]*)\)", str(dep_cell))
                    if dep_pattern:
                        dep_prefix = dep_pattern.group(1).strip() if dep_pattern.group(1) else None
                        dep_details = dep_pattern.group(2).strip() if dep_pattern.group(2) else None
                        if dep_details:
                            dep_time_match = re.search(r"DEP-([\s\S]*)-ZZZZ(\d{4})", dep_details)
                            departure_time = dep_time_match.group(2) if dep_time_match else None

            # Обработка столбца ARR (Arrival)
            if pd.notna(arr_cell):
                if file_path.name == "2025.xlsx":
                    arr_time_match = re.search(r"-ATA[\s]*(\d{4})", str(arr_cell))
                    arrival_time = arr_time_match.group(1) if arr_time_match else None
                elif file_path.name == "2024.xlsx":
                    arr_pattern = re.search(r"([\s\S]*)\(([\s\S]*)\)", str(arr_cell))
                    if arr_pattern:
                        arr_prefix = arr_pattern.group(1).strip() if arr_pattern.group(1) else None
                        arr_details = arr_pattern.group(2).strip() if arr_pattern.group(2) else None
                        if arr_details:
                            arr_time_match = re.search(r"ARR-([\s\S]*)-([\s\S]*)-ZZZZ(\d{4})", arr_details)
                            arrival_time = arr_time_match.group(3) if arr_time_match else None

            
            record = {
                "shr_original": sanitize_string(shr_cell),
                "dep_original": sanitize_string(dep_cell),
                "arr_original": sanitize_string(arr_cell),
                "shr_prefix": sanitize_string(shr_prefix),
                "dep_prefix": sanitize_string(dep_prefix),
                "arr_prefix": sanitize_string(arr_prefix),
                "flight_sid": sanitize_string(flight_sid),
                "aircraft_reg": sanitize_string(aircraft_reg),
                "departure_airport": sanitize_string(departure_airport),
                "destination_airport": sanitize_string(destination_airport),
                "estimated_time_enroute": sanitize_string(estimated_time_enroute),
                "operational_zone": sanitize_string(operational_zone),
                "aircraft_type": sanitize_string(aircraft_type),
                "flight_date": parse_date_string(flight_date),
                "departure_time": normalize_time_string(departure_time),
                "arrival_time": normalize_time_string(arrival_time),
                "region": sanitize_string(operational_region),
                "source_file": file_path.name
            }
            batch_records.append(record)

            if len(batch_records) >= 100:
                save_records_to_database(batch_records)
                batch_records = []

        if batch_records:
            save_records_to_database(batch_records)
        print(f"Обработка завершена. Обработано строк: {row_counter}")


if __name__ == "__main__":
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

    data_dir = Path("./data")
    for excel_file in data_dir.glob("*.xlsx"):
        process_excel_file(excel_file)
    