# -*- coding: utf-8 -*-
"""
Pipeline Closer Logistics — Viajes + Connections con regla de las 02:00
=======================================================================
VERSIÓN: v2.0-connections-only  (2026-06-02)

  TODO se calcula desde Connections (viajes, horas, aceptados, cancelados,
  % aceptación, % cancelación). El silver solo aporta metadatos del rider.
  Días sin cobertura de Connections se descartan.
"""

PIPELINE_VERSION = "v5.0-silver-movido-02h"

import os
import io
import re
import glob
import json
import gzip
import base64
from datetime import datetime, date, timedelta
import polars as pl

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# Carpetas de entrada (configurables por variable de entorno para staging)
COURIER_DAILY_DIR = os.environ.get('COURIER_DAILY_DIR', 'COURIER_DAILY')
CONNECTIONS_DIR   = os.environ.get('CONNECTIONS_DIR', 'CONNECTIONS')
RTA_DIR           = os.environ.get('RTA_DIR', 'CANCELLATIONS_RTA')

# Carpeta de salida
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', 'datos_salida')

# Nombre del parquet final (en staging usamos otro para no pisar producción)
SILVER_NAME = os.environ.get('SILVER_NAME', 'rides_silver')

# Sufijo para los bronze (en staging '_STAGING' para no mezclar con producción)
BRONZE_SUFFIX = os.environ.get('BRONZE_SUFFIX', '')

# Parquets de histórico (bronze incremental)
BRONZE_DAILY_PARQUET = os.path.join(OUTPUT_DIR, 'bronze_daily' + BRONZE_SUFFIX + '.parquet')
BRONZE_CONN_PARQUET  = os.path.join(OUTPUT_DIR, 'bronze_connections' + BRONZE_SUFFIX + '.parquet')
BRONZE_RTA_PARQUET   = os.path.join(OUTPUT_DIR, 'bronze_rta' + BRONZE_SUFFIX + '.parquet')

# Salidas finales
SILVER_PARQUET = os.path.join(OUTPUT_DIR, SILVER_NAME + '.parquet')

# Ventana de reproceso: cuántas semanas hacia atrás recalcular silver+ajuste.
# 3 semanas cubre la regla de 2 semanas del dashboard + margen.
REPROCESS_WEEKS = 3

# Hora de corte del día lógico (registros antes de esto → día anterior)
LOGICAL_DAY_CUTOFF_HOUR = 2

# Patrones de archivo
DAILY_PATTERN = re.compile(r'COURIER_DAILY.*\.csv$', re.IGNORECASE)
CONN_PATTERN  = re.compile(r'connections.*\.csv$', re.IGNORECASE)
RTA_PATTERN   = re.compile(r'CANCELLATION.*\.csv$', re.IGNORECASE)

WORK_STATES = ['open', 'enroute', 'ontrip']

os.makedirs(OUTPUT_DIR, exist_ok=True)
print('=' * 64)
print(f'Pipeline Closer — {PIPELINE_VERSION}')
print('=' * 64)


# =============================================================================
# UTILIDADES
# =============================================================================

def extract_ts(name):
    """Extrae timestamp de un nombre tipo ..._20260601_163355.csv"""
    m = re.search(r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})', name)
    if not m:
        return None
    return datetime(*(int(x) for x in m.groups()))


def detect_sep(path):
    """Detecta si el CSV usa TAB o coma mirando la primera línea."""
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        first = f.readline()
    return '\t' if first.count('\t') > first.count(',') else ','


# =============================================================================
# 1. BRONZE — COURIER_DAILY (incremental)
# =============================================================================

CANONICAL_DAILY = [
    'weekstr', 'datestr', 'driver_uuid', 'driver_name', 'driver_number', 'driver_email',
    'fleet_name', 'city_id', 'city_name', 'market_name', 'form_factor',
    'online_hours', 'active_hours', 'open_hours',
    'enroute_p2_hours', 'ontrip_p3_hours', 'unavailable_hours',
    'num_of_trips', 'single_trips_total', 'late_p2_trips', 'late_p3_trips',
    'accept_trips', 'reject_trips', 'cancel_trips', 'cancel_not_at_fault_trips',
    'p2_km', 'p2_min', 'p2_km_avg', 'p2_min_avg', 'p3_km', 'p3_min', 'p3_km_avg', 'p3_min_avg',
    'total_km', 'total_min', 'total_km_avg', 'total_min_avg',
]
DAILY_TEXT = {'driver_uuid', 'driver_name', 'driver_number', 'driver_email',
              'fleet_name', 'city_name', 'market_name', 'form_factor'}
DAILY_DATE = {'weekstr', 'datestr'}


def parse_daily_csv(filepath, file_name, file_ts):
    sep = detect_sep(filepath)
    df = pl.read_csv(
        filepath, separator=sep, infer_schema_length=10000,
        try_parse_dates=False, null_values=['', 'NA', 'null', 'NULL', '\\N'],
        truncate_ragged_lines=True,
    )
    for col in CANONICAL_DAILY:
        if col not in df.columns:
            if col in DAILY_DATE:   df = df.with_columns(pl.lit(None).cast(pl.Datetime).alias(col))
            elif col in DAILY_TEXT: df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            else:                   df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
    df = df.select(CANONICAL_DAILY)
    for col in DAILY_DATE:
        df = df.with_columns(pl.col(col).str.to_datetime(strict=False).alias(col))
    for col in CANONICAL_DAILY:
        if col not in DAILY_DATE and col not in DAILY_TEXT:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
    df = df.with_columns([
        pl.lit(file_name).alias('file_name'),
        pl.lit(file_ts).alias('file_date'),
    ])
    return df


def ingest_bronze_daily():
    # Histórico previo
    processed = set()
    existing = None
    if os.path.exists(BRONZE_DAILY_PARQUET):
        existing = pl.read_parquet(BRONZE_DAILY_PARQUET)
        processed = set(existing['file_name'].unique().to_list())
        print(f"[daily] Bronze previo: {len(existing):,} filas, {len(processed)} archivos ya procesados")
    else:
        print("[daily] Sin bronze previo (primera ejecución)")

    # Archivos en disco
    files = []
    for path in glob.glob(os.path.join(COURIER_DAILY_DIR, '*.csv')):
        name = os.path.basename(path)
        if not DAILY_PATTERN.search(name):
            continue
        files.append({'path': path, 'name': name, 'ts': extract_ts(name) or datetime.min})

    # Quedarse con el más reciente por día (igual que el pipeline viejo)
    por_dia = {}
    for f in files:
        d = f['ts'].date()
        if d not in por_dia or f['ts'] > por_dia[d]['ts']:
            por_dia[d] = f
    candidatos = sorted(por_dia.values(), key=lambda f: f['ts'])

    nuevos = [f for f in candidatos if f['name'] not in processed]
    print(f"[daily] Archivos nuevos a procesar: {len(nuevos)}")

    new_dfs = []
    for i, f in enumerate(nuevos, 1):
        try:
            df = parse_daily_csv(f['path'], f['name'], f['ts'])
            new_dfs.append(df)
            print(f"  [{i}/{len(nuevos)}] {f['name']}: {len(df):,} filas")
        except Exception as e:
            print(f"  [{i}/{len(nuevos)}] {f['name']}: ERROR {e}")

    parts = []
    if existing is not None: parts.append(existing)
    if new_dfs:              parts.append(pl.concat(new_dfs, how='vertical_relaxed'))
    if not parts:
        return None
    bronze = pl.concat(parts, how='vertical_relaxed') if len(parts) > 1 else parts[0]
    bronze.write_parquet(BRONZE_DAILY_PARQUET, compression='zstd')
    print(f"[daily] Bronze guardado: {len(bronze):,} filas")
    return bronze


# =============================================================================
# 2. BRONZE — CONNECTIONS (incremental)
# =============================================================================

CONN_COLS = ['courier_uuid', 'courier_name', 'contact_number', 'fleet_name', 'status',
             'datestr', 'start_time', 'end_time', 'job_daily_rank']


def parse_conn_csv(filepath, file_name):
    sep = detect_sep(filepath)
    # infer_schema=False lee TODO como texto. Evita errores como
    # "could not parse 613368445.0 as i64" cuando el móvil viene con .0,
    # o problemas si una columna mezcla tipos entre archivos. Convertimos
    # los tipos que necesitemos a mano más abajo.
    df = pl.read_csv(
        filepath, separator=sep, infer_schema=False,
        null_values=['', 'NA', 'null', 'NULL', '\\N'],
        truncate_ragged_lines=True,
    )
    # Conservar solo columnas que nos interesan (si existen)
    keep = [c for c in CONN_COLS if c in df.columns]
    df = df.select(keep)
    # Normalizar tipos clave a texto para dedup estable
    for c in ['courier_uuid', 'status', 'start_time', 'end_time']:
        if c in df.columns:
            df = df.with_columns(pl.col(c).cast(pl.Utf8, strict=False))
    df = df.with_columns(pl.lit(file_name).alias('conn_file'))
    return df


def ingest_bronze_connections():
    processed = set()
    existing = None
    if os.path.exists(BRONZE_CONN_PARQUET):
        existing = pl.read_parquet(BRONZE_CONN_PARQUET)
        processed = set(existing['conn_file'].unique().to_list())
        print(f"[conn] Bronze previo: {len(existing):,} filas, {len(processed)} archivos procesados")
    else:
        print("[conn] Sin bronze previo (primera ejecución)")

    files = []
    for path in glob.glob(os.path.join(CONNECTIONS_DIR, '*.csv')):
        name = os.path.basename(path)
        if not CONN_PATTERN.search(name):
            continue
        files.append({'path': path, 'name': name})

    nuevos = [f for f in files if f['name'] not in processed]
    print(f"[conn] Archivos nuevos a procesar: {len(nuevos)}")

    new_dfs = []
    for i, f in enumerate(nuevos, 1):
        try:
            df = parse_conn_csv(f['path'], f['name'])
            new_dfs.append(df)
            print(f"  [{i}/{len(nuevos)}] {f['name']}: {len(df):,} filas")
        except Exception as e:
            print(f"  [{i}/{len(nuevos)}] {f['name']}: ERROR {e}")

    parts = []
    if existing is not None: parts.append(existing)
    if new_dfs:              parts.append(pl.concat(new_dfs, how='vertical_relaxed'))
    if not parts:
        return None
    bronze = pl.concat(parts, how='vertical_relaxed') if len(parts) > 1 else parts[0]

    # Dedup incremental: (courier_uuid, start_time, status) — los archivos pueden solapar
    before = len(bronze)
    bronze = bronze.unique(subset=['courier_uuid', 'start_time', 'status'], keep='first')
    print(f"[conn] Dedup: {before:,} → {len(bronze):,} filas")

    bronze.write_parquet(BRONZE_CONN_PARQUET, compression='zstd')
    print(f"[conn] Bronze guardado: {len(bronze):,} filas")
    return bronze


# =============================================================================
# 2b. BRONZE — CANCELLATIONS_RTA (incremental) — pedidos individuales
# =============================================================================

RTA_COLS = ['timestamp', 'courier_uuid', 'offer_id', 'courier_action',
            'contact_number', 'email', 'fleet_name']


def parse_rta_csv(filepath, file_name):
    sep = detect_sep(filepath)
    df = pl.read_csv(
        filepath, separator=sep, infer_schema=False,
        null_values=['', 'NA', 'null', 'NULL', '\\N'],
        truncate_ragged_lines=True,
    )
    keep = [c for c in RTA_COLS if c in df.columns]
    df = df.select(keep)
    df = df.with_columns(pl.lit(file_name).alias('rta_file'))
    return df


def ingest_bronze_rta():
    processed = set()
    existing = None
    if os.path.exists(BRONZE_RTA_PARQUET):
        existing = pl.read_parquet(BRONZE_RTA_PARQUET)
        processed = set(existing['rta_file'].unique().to_list())
        print(f"[rta] Bronze previo: {len(existing):,} filas, {len(processed)} archivos procesados")
    else:
        print("[rta] Sin bronze previo (primera ejecución)")

    files = []
    for path in glob.glob(os.path.join(RTA_DIR, '*.csv')):
        name = os.path.basename(path)
        if not RTA_PATTERN.search(name):
            continue
        files.append({'path': path, 'name': name})

    nuevos = [f for f in files if f['name'] not in processed]
    print(f"[rta] Archivos nuevos a procesar: {len(nuevos)}")

    new_dfs = []
    for i, f in enumerate(nuevos, 1):
        try:
            df = parse_rta_csv(f['path'], f['name'])
            new_dfs.append(df)
        except Exception as e:
            print(f"  [{i}/{len(nuevos)}] {f['name']}: ERROR {e}")

    parts = []
    if existing is not None: parts.append(existing)
    if new_dfs:              parts.append(pl.concat(new_dfs, how='vertical_relaxed'))
    if not parts:
        return None
    bronze = pl.concat(parts, how='vertical_relaxed') if len(parts) > 1 else parts[0]

    # Dedup incremental por offer_id (cada pedido es único; archivos solapan)
    before = len(bronze)
    bronze = bronze.unique(subset=['offer_id'], keep='first')
    print(f"[rta] Dedup por offer_id: {before:,} → {len(bronze):,} filas")

    bronze.write_parquet(BRONZE_RTA_PARQUET, compression='zstd')
    print(f"[rta] Bronze guardado: {len(bronze):,} filas")
    return bronze


# =============================================================================
# 3. SILVER (dedup del daily) + recorte a ventana reciente
# =============================================================================

def reconstruct_rta(bronze_rta):
    """
    Calcula, por (courier_uuid, FECHA REAL), la FRACCIÓN de pedidos que ocurrió
    antes de las 02:00 (madrugada). Esa fracción se usará para mover esa parte
    de los totales del SILVER al día anterior.

    Cada offer_id es un pedido individual. Solo contamos ACCEPT (pedidos que
    aceptó). Cada pedido se ancla por su timestamp.
      - frac_rta = pedidos ACCEPT de madrugada / pedidos ACCEPT totales del día
    """
    r = bronze_rta.with_columns(
        pl.col('timestamp').str.to_datetime(strict=False).alias('ts')
    ).filter(pl.col('ts').is_not_null())

    # Solo ACCEPT (pedidos aceptados). FECHA REAL del calendario.
    r = r.filter(pl.col('courier_action') == 'ACCEPT')
    r = r.with_columns([
        pl.col('ts').dt.date().alias('fecha_real'),
        (pl.col('ts').dt.hour() < LOGICAL_DAY_CUTOFF_HOUR).alias('es_madrugada'),
    ])

    g = (
        r.group_by(['courier_uuid', 'fecha_real']).agg([
            pl.len().alias('rta_total'),
            pl.col('es_madrugada').sum().alias('rta_madrugada'),
        ])
    )
    g = g.with_columns(
        pl.when(pl.col('rta_total') > 0)
          .then(pl.col('rta_madrugada') / pl.col('rta_total'))
          .otherwise(0.0).alias('frac_rta')
    )
    g = g.rename({'fecha_real': 'dia'})
    return g.select(['courier_uuid', 'dia', 'frac_rta', 'rta_total', 'rta_madrugada'])


# =============================================================================
# SILVER
# =============================================================================

def build_silver(bronze_daily):
    """
    Construye el silver con UNA fila por (driver_uuid, día), pero SUMANDO los
    turnos partidos / zonas distintas del mismo día (p. ej. CARABANCHEL + CENTRO).

    Dos pasos:
      1) Dedup de VERSIONES: el mismo (uuid, día, ciudad) puede venir repetido en
         varios CSV (re-exportaciones). Nos quedamos con la fila del file_date más
         reciente (y, si empatan, la de más num_of_trips).
      2) SUMA por (uuid, día): se suman horas, viajes, km y min de todas las zonas.
         Las métricas promedio/derivadas (_avg) se RECALCULAN sobre los totales,
         no se suman.
    """
    base = bronze_daily.filter(
        pl.col('datestr').is_not_null() & pl.col('driver_uuid').is_not_null()
    )

    # --- Paso 1: quedarnos con la versión más reciente de cada (uuid, día, ciudad) ---
    # city_id distingue zonas; si no existe, usamos city_name; si tampoco, cadena vacía.
    zona_col = 'city_id' if 'city_id' in base.columns else (
        'city_name' if 'city_name' in base.columns else None)
    subset_version = ['driver_uuid', 'datestr'] + ([zona_col] if zona_col else [])
    base = (
        base.sort(
            ['driver_uuid', 'datestr', 'file_date', 'num_of_trips'],
            descending=[False, False, True, True],
            nulls_last=True,
        )
        .unique(subset=subset_version, keep='first', maintain_order=True)
    )

    # --- Paso 2: sumar todas las zonas del mismo (uuid, día) ---
    # Columnas que se SUMAN (cantidades absolutas)
    SUM_COLS = [c for c in [
        'online_hours', 'active_hours', 'open_hours',
        'enroute_p2_hours', 'ontrip_p3_hours', 'unavailable_hours',
        'num_of_trips', 'single_trips_total', 'late_p2_trips', 'late_p3_trips',
        'accept_trips', 'reject_trips', 'cancel_trips', 'cancel_not_at_fault_trips',
        'p2_km', 'p2_min', 'p3_km', 'p3_min', 'total_km', 'total_min',
    ] if c in base.columns]

    # Columnas de texto/identidad: tomamos la primera (la de la zona con más viajes,
    # porque venimos ordenados por num_of_trips desc dentro del día)
    FIRST_COLS = [c for c in [
        'weekstr', 'driver_name', 'driver_number', 'driver_email',
        'fleet_name', 'city_id', 'city_name', 'market_name', 'form_factor',
    ] if c in base.columns]

    aggs = [pl.col(c).sum().alias(c) for c in SUM_COLS]
    aggs += [pl.col(c).first().alias(c) for c in FIRST_COLS]

    silver = (
        base.sort(['driver_uuid', 'datestr', 'num_of_trips'],
                  descending=[False, False, True], nulls_last=True)
        .group_by(['driver_uuid', 'datestr'], maintain_order=True)
        .agg(aggs)
    )

    # --- Recalcular promedios (_avg) sobre los totales sumados ---
    def avg_expr(num, den, name):
        if num in silver.columns and den in silver.columns:
            return (pl.when(pl.col(den) > 0)
                      .then(pl.col(num) / pl.col(den))
                      .otherwise(0.0).alias(name))
        return None
    recalcs = [
        avg_expr('p2_km', 'num_of_trips', 'p2_km_avg'),
        avg_expr('p2_min', 'num_of_trips', 'p2_min_avg'),
        avg_expr('p3_km', 'num_of_trips', 'p3_km_avg'),
        avg_expr('p3_min', 'num_of_trips', 'p3_min_avg'),
        avg_expr('total_km', 'num_of_trips', 'total_km_avg'),
        avg_expr('total_min', 'num_of_trips', 'total_min_avg'),
    ]
    recalcs = [r for r in recalcs if r is not None]
    if recalcs:
        silver = silver.with_columns(recalcs)

    # Recorte a la ventana reciente para reprocesar rápido
    max_day = silver.select(pl.col('datestr').max()).item()
    if max_day is not None:
        cutoff = max_day - timedelta(weeks=REPROCESS_WEEKS)
        silver = silver.filter(pl.col('datestr') >= cutoff)
        print(f"[silver] Ventana: {cutoff.date()} → {max_day.date()} ({len(silver):,} filas)")
    return silver


# =============================================================================
# 4. AJUSTE desde CONNECTIONS (regla de las 02:00)
# =============================================================================

def reconstruct_connections(bronze_conn):
    """
    Calcula, por (courier_uuid, FECHA REAL), la FRACCIÓN de horas conectado
    que ocurrió antes de las 02:00 (madrugada). Esa fracción se usará para mover
    esa parte de las horas del SILVER al día anterior.

    Horas = duración de open+enroute+ontrip (filas con end válido).
    Cada fila se ancla por su start_time.
      - frac_horas = horas de madrugada / horas totales del día
    """
    conn = bronze_conn.with_columns([
        pl.col('start_time').str.to_datetime(strict=False).alias('start_dt'),
        pl.col('end_time').str.to_datetime(strict=False).alias('end_dt'),
    ]).filter(pl.col('start_dt').is_not_null())

    # Solo estados que cuentan como horas, con fin válido
    conn = conn.filter(pl.col('status').is_in(WORK_STATES) & pl.col('end_dt').is_not_null())
    conn = conn.with_columns([
        pl.col('start_dt').dt.date().alias('fecha_real'),
        (pl.col('start_dt').dt.hour() < LOGICAL_DAY_CUTOFF_HOUR).alias('es_madrugada'),
        ((pl.col('end_dt') - pl.col('start_dt')).dt.total_seconds() / 3600).alias('dur_h'),
    ])

    g = (
        conn.group_by(['courier_uuid', 'fecha_real']).agg([
            pl.col('dur_h').sum().alias('horas_total'),
            (pl.col('dur_h') * pl.col('es_madrugada').cast(pl.Float64)).sum().alias('horas_madrugada'),
        ])
    )
    g = g.with_columns(
        pl.when(pl.col('horas_total') > 0)
          .then(pl.col('horas_madrugada') / pl.col('horas_total'))
          .otherwise(0.0).alias('frac_horas')
    )
    g = g.rename({'fecha_real': 'dia'})
    return g.select(['courier_uuid', 'dia', 'frac_horas', 'horas_total', 'horas_madrugada'])


def apply_adjustment(silver, rta, conn):
    """
    Modelo PROPORCIONAL sobre el SILVER (regla 02:00).

    El SILVER manda en TODOS los totales (num_of_trips, accept, cancel, horas)
    porque ya viene correcto de Uber. Lo único que hacemos es MOVER la parte
    de madrugada (< 02:00) de cada día al día anterior.

    La fracción de madrugada se calcula con timestamps reales:
      - PEDIDOS (viajes, accept, cancel) → fracción de RTA (frac_rta)
      - HORAS (online_hours, p2, p3, etc.) → fracción de Connections (frac_horas)

    Para cada (rider, día D):
      mover_pedidos = total_silver(D) * frac_rta(D)   → de D a D-1
      mover_horas   = horas_silver(D) * frac_horas(D) → de D a D-1
    Si un día no tiene RTA/Connections, su fracción es 0 → no se mueve nada.

    Pedidos/viajes: enteros (round). Horas: decimal.
    % aceptación/cancelación: se recalculan con los totales ya movidos,
    usando las fórmulas oficiales del silver:
      % Aceptación  = accept / (accept + reject)
      % Cancelación = cancel / accept
    """
    silver = silver.with_columns(pl.col('datestr').dt.date().alias('_dia'))

    # --- Unir fracciones de RTA y Connections al silver por (uuid, día) ---
    s = silver
    if rta is not None and len(rta) > 0:
        s = s.join(rta.select(['courier_uuid', 'dia', 'frac_rta']),
                   left_on=['driver_uuid', '_dia'], right_on=['courier_uuid', 'dia'], how='left')
    else:
        s = s.with_columns(pl.lit(0.0).alias('frac_rta'))
    if conn is not None and len(conn) > 0:
        s = s.join(conn.select(['courier_uuid', 'dia', 'frac_horas']),
                   left_on=['driver_uuid', '_dia'], right_on=['courier_uuid', 'dia'], how='left')
    else:
        s = s.with_columns(pl.lit(0.0).alias('frac_horas'))

    s = s.with_columns([
        pl.col('frac_rta').fill_null(0.0),
        pl.col('frac_horas').fill_null(0.0),
    ])

    # Columnas de pedidos que se mueven con frac_rta
    PEDIDO_COLS = [c for c in ['num_of_trips', 'single_trips_total', 'accept_trips',
                               'reject_trips', 'cancel_trips', 'cancel_not_at_fault_trips',
                               'late_p2_trips', 'late_p3_trips'] if c in s.columns]
    # Columnas de horas/distancia que se mueven con frac_horas
    HORA_COLS = [c for c in ['online_hours', 'active_hours', 'open_hours',
                             'enroute_p2_hours', 'ontrip_p3_hours', 'unavailable_hours',
                             'p2_km', 'p2_min', 'p3_km', 'p3_min', 'total_km', 'total_min'] if c in s.columns]

    # --- Cantidad que se mueve al día anterior ---
    mv_exprs = []
    for c in PEDIDO_COLS:
        mv_exprs.append((pl.col(c).fill_null(0) * pl.col('frac_rta')).round(0).alias('mv_' + c))
    for c in HORA_COLS:
        mv_exprs.append((pl.col(c).fill_null(0) * pl.col('frac_horas')).alias('mv_' + c))
    s = s.with_columns(mv_exprs)

    # --- Construir los movimientos (lo que entra en D-1) ---
    move_cols = ['mv_' + c for c in PEDIDO_COLS + HORA_COLS]
    movidos = s.select(
        ['driver_uuid'] +
        [(pl.col('_dia') - pl.duration(days=1)).alias('_dia')] +
        [pl.col('mv_' + c).alias('in_' + c) for c in PEDIDO_COLS + HORA_COLS]
    )
    movidos = movidos.group_by(['driver_uuid', '_dia']).agg(
        [pl.col('in_' + c).sum() for c in PEDIDO_COLS + HORA_COLS]
    )

    # --- Restar de cada día lo que sale, sumar lo que entra ---
    # Primero restamos
    s = s.with_columns(
        [(pl.col(c).fill_null(0) - pl.col('mv_' + c)).alias(c) for c in PEDIDO_COLS + HORA_COLS]
    )
    # Unimos lo que entra del día siguiente
    s = s.join(movidos, on=['driver_uuid', '_dia'], how='left')
    s = s.with_columns(
        [(pl.col(c) + pl.col('in_' + c).fill_null(0)).alias(c) for c in PEDIDO_COLS + HORA_COLS]
    )

    # --- Recalcular métricas derivadas con los totales ya movidos ---
    s = s.with_columns([
        pl.when(pl.col('online_hours') > 0)
          .then(pl.col('num_of_trips') / pl.col('online_hours'))
          .otherwise(0.0).alias('tph_adj'),
    ])
    if 'accept_trips' in s.columns and 'reject_trips' in s.columns:
        s = s.with_columns(
            pl.when((pl.col('accept_trips') + pl.col('reject_trips')) > 0)
              .then(pl.col('accept_trips') / (pl.col('accept_trips') + pl.col('reject_trips')) * 100)
              .otherwise(0.0).alias('pct_aceptacion')
        )
    if 'cancel_trips' in s.columns and 'accept_trips' in s.columns:
        s = s.with_columns(
            pl.when(pl.col('accept_trips') > 0)
              .then(pl.col('cancel_trips') / pl.col('accept_trips') * 100)
              .otherwise(0.0).alias('pct_cancelacion')
        )

    # Marcar si la fila tuvo movimiento (para inspección)
    s = s.with_columns([
        ((pl.col('frac_rta') > 0) | (pl.col('frac_horas') > 0) |
         pl.col('in_num_of_trips').is_not_null()).alias('ajustado_connections'),
        pl.col('frac_rta').alias('_frac_rta_dbg'),
        pl.col('frac_horas').alias('_frac_horas_dbg'),
    ])

    # Limpiar auxiliares
    aux = ['frac_rta', 'frac_horas'] + ['mv_' + c for c in PEDIDO_COLS + HORA_COLS] + \
          ['in_' + c for c in PEDIDO_COLS + HORA_COLS]
    s = s.drop([c for c in aux if c in s.columns])
    return s


# =============================================================================
# 5. MAIN
# =============================================================================

def main():
    # --- Bronze ---
    bronze_daily = ingest_bronze_daily()
    if bronze_daily is None:
        print("\n✗ No hay datos de COURIER_DAILY. Abortando.")
        return
    bronze_conn = ingest_bronze_connections()
    bronze_rta  = ingest_bronze_rta()

    # --- Silver ---
    silver = build_silver(bronze_daily)

    # --- Reconstrucción de cada fuente por día lógico ---
    recon_conn = None
    if bronze_conn is not None and len(bronze_conn) > 0:
        recon_conn = reconstruct_connections(bronze_conn)
        print(f"[conn] Reconstrucción: {len(recon_conn):,} combinaciones (rider, día)")

    recon_rta = None
    if bronze_rta is not None and len(bronze_rta) > 0:
        recon_rta = reconstruct_rta(bronze_rta)
        print(f"[rta] Reconstrucción: {len(recon_rta):,} combinaciones (rider, día)")

    # --- Ajuste proporcional sobre el silver (regla 02:00) ---
    if recon_conn is not None or recon_rta is not None:
        final = apply_adjustment(silver, recon_rta, recon_conn)
        n_mov = final.filter(pl.col('ajustado_connections')).height
        print(f"[ajuste] Filas con movimiento de madrugada: {n_mov:,} / {len(final):,}")
    else:
        print("[ajuste] Sin Connections ni RTA — silver sin ajustar")
        final = silver.with_columns([
            pl.when(pl.col('online_hours') > 0)
              .then(pl.col('num_of_trips') / pl.col('online_hours'))
              .otherwise(0.0).alias('tph_adj'),
            pl.lit(False).alias('ajustado_connections'),
            pl.lit(0.0).alias('_frac_rta_dbg'),
            pl.lit(0.0).alias('_frac_horas_dbg'),
        ])
        if '_dia' in final.columns:
            final = final.drop('_dia')

    # --- Quitar filas de días vacíos (0 viajes Y 0 horas) ---
    antes = len(final)
    final = final.filter(
        ~((pl.col('num_of_trips').fill_null(0) == 0) &
          (pl.col('online_hours').fill_null(0) == 0))
    )
    quitadas = antes - len(final)
    if quitadas > 0:
        print(f"[limpieza] Filas vacías eliminadas (0 viajes y 0 horas): {quitadas}")

    # --- Salida: SOLO el parquet (lo que lee la app). Sin CSV ni dashboard. ---
    final.write_parquet(SILVER_PARQUET, compression='zstd')
    print(f"\n✓ Parquet final: {SILVER_PARQUET} ({len(final):,} filas)")

    # NOTA: el pipeline NO genera dashboard.html. El dashboard de producción
    # es la app React, que lee el parquet directamente. No tocamos ese archivo.

    # --- Chequeo de integridad: una sola fila por (rider, día) ---
    dups = (final.with_columns(pl.col('datestr').dt.date().alias('_d'))
                 .group_by(['driver_uuid', '_d']).agg(pl.len().alias('n'))
                 .filter(pl.col('n') > 1))
    if len(dups) > 0:
        print(f"⚠ ADVERTENCIA: hay {len(dups)} duplicados (rider, día)")
    else:
        print("✓ Integridad OK: una fila por rider y día")

    print("\n¡Proceso completado!")


if __name__ == '__main__':
    main()
