import re
import sys
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

# === Функции для обработки даты и времени ===
def clean_date(date_str):
    if pd.isna(date_str):
        return None
    try:
        return pd.to_datetime(str(date_str), format="%Y%m%d").date()
    except Exception:
        return None

def clean_time(time_str):
    if pd.isna(time_str):
        return None
    time_str = str(time_str).zfill(4)
    try:
        return pd.to_datetime(time_str, format="%H%M").time()
    except Exception:
        return None


def clean_value(val):
    """Удаляет лишние символы ) и /) и пробелы"""
    if pd.isna(val):
        return None
    return str(val).strip().rstrip(")/")


# === Парсинг формата 2024 ===
def parse_2024_from_excel(df, file_name):
    flights = []
    sid, reg, dep, dest, dof, eet, zona, typ = None, None, None, None, None, None, None, None
    dep_time, arr_time = None, None
    (shr_outside_parenth, shr_inside_parenth,
     dep_outside_parenth, dep_inside_parenth,
     arr_outside_parenth, arr_inside_parenth) = None, None, None, None, None, None

    for col in ["SHR", "DEP", "ARR"]:
        if col not in df.columns:
            df[col] = None

    for _, row in df.iterrows():
        shr_text, dep_text, arr_text = row["SHR"], row["DEP"], row["ARR"]

        # SHR
        if pd.notna(shr_text):
            shr_match = re.search(r"([\s\S]*)\(([\s\S]*)\)", shr_text)

            if pd.notna(shr_match):
                shr_outside_parenth = shr_match.group(1) if shr_match.group(1) else None
                shr_inside_parenth = shr_match.group(2) if shr_match.group(2) else None

                if pd.notna(shr_inside_parenth):
                    sid_match = re.search(r"SID/(\S+)", shr_inside_parenth)
                    sid = sid_match.group(1) if sid_match else None

                    reg_match = re.search(r"REG/(\S+)", shr_inside_parenth)
                    reg = reg_match.group(1) if reg_match else None

                    dep_match = re.search(r"DEP/(\S+)", shr_inside_parenth)
                    dep = dep_match.group(1) if dep_match else None

                    dest_match = re.search(r"DEST/(\S+)", shr_inside_parenth)
                    dest = dest_match.group(1) if dest_match else None

                    dof_match = re.search(r"DOF/(\S+)", shr_inside_parenth)
                    dof = dof_match.group(1) if dof_match else None

                    eet_match = re.search(r"EET/(\S+)", shr_inside_parenth)
                    eet = eet_match.group(1) if eet_match else None

                    typ_match = re.search(r"TYP/(\S+)", shr_inside_parenth)
                    typ = typ_match.group(1) if typ_match else None

                    zona_match = re.search(r"/ZONA ([\s\S]+)/", shr_inside_parenth)
                    zona = zona_match.group(1) if zona_match else None

        # DEP
        if pd.notna(dep_text):
            dep_match = re.search(r"([\s\S]*)\(([\s\S]*)\)", dep_text)

            if pd.notna(dep_match):
                dep_outside_parenth = dep_match.group(1) if dep_match.group(1) else None
                dep_inside_parenth = dep_match.group(2) if dep_match.group(2) else None

                if pd.notna(dep_inside_parenth):
                    dep_time_match = re.search(r"DEP-([\s\S]*)-ZZZZ(\d{4})", dep_inside_parenth)
                    dep_time = dep_time_match.group(1) if dep_time_match else None


        # ARR
        if pd.notna(arr_text) and flights:
            arr_match = re.search(r"([\s\S]*)\(([\s\S]*)\)", arr_text)

            if pd.notna(arr_match):
                arr_outside_parenth = arr_match.group(1) if arr_match.group(1) else None
                arr_inside_parenth = arr_match.group(2) if arr_match.group(2) else None

                if pd.notna(arr_inside_parenth):
                    arr_time_match = re.search(r"ARR-([\s\S]*)-([\s\S]*)-ZZZZ(\d{4})", arr_inside_parenth)
                    arr_time = arr_time_match.group(1) if arr_time_match else None

        flights.append({
            "SHR_COL": shr_text if shr_text else None,
            "DEP_COL": dep_text if dep_text else None,
            "ARR_COL": arr_text if arr_text else None,
            "F1": clean_value(shr_outside_parenth) if shr_outside_parenth else None,
            "F2": clean_value(dep_outside_parenth) if dep_outside_parenth else None,
            "F3": clean_value(arr_outside_parenth) if arr_outside_parenth else None,
            "SID": clean_value(sid) if sid else None,
            "REG": clean_value(reg) if reg else None,
            "DEP": clean_value(dep) if dep else None,
            "DEST": clean_value(dest) if dest else None,
            "EET": clean_value(eet) if eet else None,
            "ZONA": clean_value(zona),
            "TYP": clean_value(typ),
            "DOF": clean_date(dof) if dof else None,
            "DEP_TIME": clean_time(dep_time) if dep_time else None,
            "ARR_TIME": clean_time(arr_time) if arr_time else None,
            "FILE": file_name
        })

    return flights

# === Парсинг формата 2025 ===
def parse_2025_from_excel(df, file_name):
    flights = []

    return flights

# === Определение формата файла ===
def parse_excel_file(file_path):
    all_flights = []
    xls = pd.ExcelFile(file_path)
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        if any(col in df.columns for col in ["DEP", "ARR"]):
            all_flights.extend(parse_2024_from_excel(df, file_path.name))
        #elif "SHR" in df.columns:
        #    all_flights.extend(parse_2025_from_excel(df, file_path.name))
    return all_flights

# === Основной блок ===
if __name__ == "__main__":
    folder = Path("./data")
    flights = []
    for file in folder.glob("*.xlsx"):
        print(f"Обрабатываю {file.name} ...")
        flights.extend(parse_excel_file(file))
    df = pd.DataFrame(flights)
    df.to_excel("./data/flights.xlsx", index=False)
    df.to_csv("./data/flights.csv", index=False, encoding="utf-8-sig")
    print(f"✅ Данные сохранены в flights.xlsx и flights.csv (строк: {len(df)})")

    # PostgreSQL
    db_user = "postgres"
    db_pass = "secret"
    db_host = "db"
    db_port = "5432"
    db_name = "flights_db"

    engine = create_engine(f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

    create_table_sql = """
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
        file TEXT
    );
    """

    # create_index_sql = """
    # DO $$
    # BEGIN
    #     IF NOT EXISTS (
    #         SELECT 1
    #         FROM pg_indexes
    #         WHERE schemaname = 'public'
    #           AND indexname = 'flights_sid_date_idx'
    #     ) THEN
    #         CREATE UNIQUE INDEX flights_sid_date_idx
    #             ON flights (sid, date);
    #     END IF;
    # END
    # $$;
    # """

    with engine.connect() as conn:
        try:
            print("Создаём таблицу и индекс при необходимости...")
            conn.execute(text(create_table_sql))
            #conn.execute(text(create_index_sql))
            conn.commit()
            print(f"Общее количество строк для вставки: {len(df)}")
            df.columns = [c.lower() for c in df.columns]
            df.to_sql("flights", engine, if_exists="append", index=False, method="multi")
            print(f"🚀 Данные успешно вставлены в таблицу flights")
        except IntegrityError as e:
            print("⚠️ Вставка прервана из-за дубликатов (уникальный индекс sid+date+file)")
            print(str(e))
            sys.exit(1)
        except Exception as e:
            print("❌ Произошла ошибка при вставке в PostgreSQL")
            print(str(e))
            sys.exit(1)
