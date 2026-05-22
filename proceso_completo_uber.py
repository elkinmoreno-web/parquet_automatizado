# -*- coding: utf-8 -*-
"""
Proceso Automatizado Completo (Bronze ➔ Silver ➔ Dashboard)
Adaptado para GitHub Actions + Rclone (Ejecución Local)
"""

import os
import io
import json
import re
import gzip
import base64
from datetime import datetime, timezone, date, timedelta
import polars as pl

# --- CONFIGURACIÓN DE CARPETAS LOCALES ---
# Rclone descargará los datos aquí y subirá los resultados desde aquí
INPUT_DIR = 'datos_entrada'
OUTPUT_DIR = 'datos_salida'

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

MIN_FILE_DATE = '2026-03-13'
FILE_PATTERN = re.compile(r'^COURIER_DAILY_CLOSERLOGISTICS_\d{8}_\d{6}\.csv$')

PARQUET_FILENAME = 'rides_raw.parquet'
SILVER_FILENAME = 'rides_silver.parquet'
HTML_FILENAME = 'dashboard.html'
CSV_FILENAME = 'rides_silver.csv'

DASH_COLS = [
    'datestr', 'weekstr', 'driver_uuid', 'driver_name', 'driver_email', 'driver_number',
    'city_name', 'market_name', 'form_factor', 'online_hours', 'active_hours', 'open_hours',
    'enroute_p2_hours', 'ontrip_p3_hours', 'unavailable_hours', 'num_of_trips', 'single_trips_total',
    'late_p2_trips', 'late_p3_trips', 'accept_trips', 'reject_trips', 'cancel_trips', 'cancel_not_at_fault_trips',
    'p2_km', 'p2_min', 'p3_km', 'p3_min', 'total_km', 'total_min',
]

FACTORIZE = {'driver_uuid','driver_name','driver_email','driver_number',
             'city_name','market_name','form_factor','datestr','weekstr'}

print('Configuración local cargada.')

# =============================================================================
# 1. CAPA BRONZE (Ingesta local)
# =============================================================================

def extract_ts(name):
    m = re.search(r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})', name)
    if not m: return None
    return datetime(*(int(x) for x in m.groups()))

# 1.1 Listar CSVs locales (ya descargados por Rclone)
all_files = os.listdir(INPUT_DIR)
valid_files = []
min_dt = datetime.fromisoformat(MIN_FILE_DATE) if MIN_FILE_DATE else None

for fname in all_files:
    if not FILE_PATTERN.match(fname): continue
    ts = extract_ts(fname)
    if ts is None or (min_dt and ts < min_dt): continue
    valid_files.append({'name': fname, 'timestamp': ts})

# Filtrar el más reciente por día
archivos_por_dia = {}
for f in valid_files:
    dia = f['timestamp'].date()
    if dia not in archivos_por_dia or f['timestamp'] > archivos_por_dia[dia]['timestamp']:
        archivos_por_dia[dia] = f

csvs = list(archivos_por_dia.values())
csvs.sort(key=lambda f: f['timestamp'])
print(f"CSVs únicos (1 por día) encontrados en local: {len(csvs)}")

# 1.2 Ver qué ficheros ya están en el Parquet previo (si existe)
existing_parquet_path = os.path.join(OUTPUT_DIR, PARQUET_FILENAME)
processed_files = set()
existing_df = None

if os.path.exists(existing_parquet_path):
    print("Parquet previo encontrado localmente. Leyendo historial...")
    existing_df = pl.read_parquet(existing_parquet_path)
    processed_files = set(existing_df['file_name'].unique().to_list())
else:
    print('No hay Parquet previo. Primera ejecución.')

new_csvs = [f for f in csvs if f['name'] not in processed_files]
print(f"CSVs nuevos a procesar: {len(new_csvs)}")

# 1.3 Parsear CSVs
CANONICAL_COLUMNS = [
    'weekstr', 'datestr', 'driver_uuid', 'driver_name', 'driver_number', 'driver_email',
    'fleet_name', 'city_id', 'city_name', 'market_name', 'form_factor', 'online_hours', 'active_hours', 'open_hours',
    'enroute_p2_hours', 'ontrip_p3_hours', 'unavailable_hours', 'num_of_trips', 'single_trips_total',
    'late_p2_trips', 'late_p3_trips', 'accept_trips', 'reject_trips', 'cancel_trips', 'cancel_not_at_fault_trips',
    'p2_km', 'p2_min', 'p2_km_avg', 'p2_min_avg', 'p3_km', 'p3_min', 'p3_km_avg', 'p3_min_avg',
    'total_km', 'total_min', 'total_km_avg', 'total_min_avg',
]
TEXT_COLS = {'driver_uuid', 'driver_name', 'driver_number', 'driver_email', 'fleet_name', 'city_name', 'market_name', 'form_factor'}
DATE_COLS = {'weekstr', 'datestr'}

def parse_csv(filepath, file_name, file_ts):
    with open(filepath, 'rb') as f:
        content = f.read()
        
    df = pl.read_csv(io.BytesIO(content), infer_schema_length=10000, try_parse_dates=False, null_values=['', 'NA', 'null', 'NULL'])
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            if col in DATE_COLS: df = df.with_columns(pl.lit(None).cast(pl.Datetime).alias(col))
            elif col in TEXT_COLS: df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            else: df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
            
    df = df.select(CANONICAL_COLUMNS)
    for col in DATE_COLS: df = df.with_columns(pl.col(col).str.to_datetime(strict=False).alias(col))
    for col in CANONICAL_COLUMNS:
        if col not in DATE_COLS and col not in TEXT_COLS:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
            
    df = df.with_columns([pl.lit(file_name).alias('file_name'), pl.lit(file_ts).alias('file_date')])
    return df

new_dfs = []
for i, f in enumerate(new_csvs, 1):
    try:
        filepath = os.path.join(INPUT_DIR, f['name'])
        print(f"[{i}/{len(new_csvs)}] {f['name']}...", end=' ')
        df = parse_csv(filepath, f['name'], f['timestamp'])
        new_dfs.append(df)
        print(f"{len(df):,} filas")
    except Exception as e:
        print(f"ERROR: {e}")

# 1.4 Concatenar y guardar
if new_dfs:
    new_all = pl.concat(new_dfs, how='vertical_relaxed')
else:
    new_all = None

if existing_df is not None and new_all is not None: final_df = pl.concat([existing_df, new_all], how='vertical_relaxed')
elif existing_df is not None: final_df = existing_df
elif new_all is not None: final_df = new_all
else: final_df = None

if final_df is not None:
    # Guardar Bronze localmente
    final_df.write_parquet(existing_parquet_path, compression='zstd')
    print(f"Bronze Parquet guardado localmente: {existing_parquet_path}")


# =============================================================================
# 2. CAPA SILVER (Deduplicación)
# =============================================================================
if final_df is not None:
    bronze = final_df
    silver = (
        bronze
        .with_columns(pl.col('file_date').max().over(['driver_uuid', 'datestr']).alias('_max_file_date'))
        .filter(pl.col('file_date') == pl.col('_max_file_date'))
        .drop('_max_file_date')
    )
    silver = silver.filter(pl.col('datestr').is_not_null() & pl.col('driver_uuid').is_not_null())
    
    # Guardar Silver Parquet y CSV localmente
    silver_parquet_path = os.path.join(OUTPUT_DIR, SILVER_FILENAME)
    silver_csv_path = os.path.join(OUTPUT_DIR, CSV_FILENAME)
    
    silver.write_parquet(silver_parquet_path, compression='zstd')
    silver.write_csv(silver_csv_path)
    print(f"Silver Parquet y CSV guardados localmente en: {OUTPUT_DIR}")


# =============================================================================
# 3. DASHBOARD HTML
# =============================================================================
if 'silver' in locals() and silver is not None:
    def build_payload(df):
        columns, dicts = {}, {}
        for col in df.columns:
            values = df[col].to_list()
            if col in FACTORIZE:
                unique, idx_map, indices = [], {}, []
                for v in values:
                    if v is None:
                        indices.append(-1)
                        continue
                    if v not in idx_map:
                        idx_map[v] = len(unique)
                        unique.append(v)
                    indices.append(idx_map[v])
                dicts[col] = unique
                columns[col] = indices
            else:
                columns[col] = [round(v, 2) if v is not None else None for v in values]
        return {'cols': columns, 'dicts': dicts}

    def generate_weeks(min_date, max_date, lookback_weeks=8):
        today = date.today()
        end = max(max_date, today)
        start = min_date - timedelta(days=lookback_weeks*7)
        start -= timedelta(days=start.weekday())
        weeks = []
        cur = start
        while cur <= end:
            week_end = cur + timedelta(days=6)
            iso = cur.isocalendar()
            weeks.append({'week_num': iso[1] if isinstance(iso, tuple) else iso.week, 'year': iso[0] if isinstance(iso, tuple) else iso.year, 'start': cur.isoformat(), 'end': week_end.isoformat()})
            cur += timedelta(days=7)
        weeks.sort(key=lambda w: w['start'], reverse=True)
        return weeks

    available_cols = [c for c in DASH_COLS if c in silver.columns]
    date_cols_present = [c for c in ('datestr', 'weekstr') if c in available_cols]
    df_dash = silver.select(available_cols)
    for c in date_cols_present:
        df_dash = df_dash.with_columns(pl.col(c).dt.strftime('%Y-%m-%d'))

    payload = build_payload(df_dash)
    min_d = silver['datestr'].min().date()
    max_d = silver['datestr'].max().date()
    weeks = generate_weeks(min_d, max_d)

    meta = {
        'total_rows': len(silver), 'n_drivers': silver['driver_uuid'].n_unique(),
        'date_min': silver['datestr'].min().strftime('%Y-%m-%dT%H:%M:%S'), 'date_max': silver['datestr'].max().strftime('%Y-%m-%dT%H:%M:%S'),
        'markets': sorted([m for m in silver['market_name'].unique().to_list() if m]), 'cities': sorted([c for c in silver['city_name'].unique().to_list() if c]),
        'form_factors': sorted([f for f in silver['form_factor'].unique().to_list() if f]), 'weeks': weeks,
        'generated_at': datetime.now(timezone.utc).isoformat(),
    }

    def compress_b64(obj):
        raw = json.dumps(obj, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        gz = gzip.compress(raw, compresslevel=9)
        return raw, gz, base64.b64encode(gz).decode('ascii')

    data_raw, data_gz, data_b64 = compress_b64(payload)
    meta_raw, meta_gz, meta_b64 = compress_b64(meta)

    TEMPLATE_HTML = "<!DOCTYPE html>\n<html lang=\"es\">\n<head>\n<meta charset=\"UTF-8\">\n<title>Riders · Dashboard</title>\n... PEGA AQUÍ EL RESTO DE TU LÍNEA GIGANTE ... </body>\n</html>\n"
    html = (
        TEMPLATE_HTML
        .replace('__DATA_B64__', data_b64)
        .replace('__META_B64__', meta_b64)
    )
    
    html_path = os.path.join(OUTPUT_DIR, HTML_FILENAME)
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Dashboard HTML guardado localmente en: {html_path}")
    print("¡Proceso completado con éxito!")

