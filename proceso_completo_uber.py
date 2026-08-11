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
from supabase import create_client

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

    # Concatenación INCREMENTAL por lotes: con cientos de CSV grandes, guardar
    # los 118+ DataFrames sueltos en una lista y concatenarlos TODOS de golpe
    # al final crea un pico de memoria enorme (lista completa + copia
    # concatenada + bronze anterior, coexistiendo a la vez) — en el runner de
    # GitHub Actions (7 GB de RAM) esto agotaba la memoria y el proceso era
    # cancelado por el sistema (SIGTERM, exit code 143). Concatenando de a
    # LOTE_TAMANO archivos, nunca hay más que un puñado de DataFrames sueltos
    # en memoria a la vez — el resultado final es idéntico, solo cambia cómo
    # se llega a él.
    LOTE_TAMANO = 15
    acumulado = existing
    lote_actual = []

    def volcar_lote():
        nonlocal acumulado, lote_actual
        if not lote_actual:
            return
        concat_lote = pl.concat(lote_actual, how='vertical_relaxed')
        acumulado = concat_lote if acumulado is None else pl.concat([acumulado, concat_lote], how='vertical_relaxed')
        lote_actual = []

    for i, f in enumerate(nuevos, 1):
        try:
            df = parse_conn_csv(f['path'], f['name'])
            lote_actual.append(df)
            print(f"  [{i}/{len(nuevos)}] {f['name']}: {len(df):,} filas")
        except Exception as e:
            print(f"  [{i}/{len(nuevos)}] {f['name']}: ERROR {e}")
        if len(lote_actual) >= LOTE_TAMANO:
            volcar_lote()
    volcar_lote()

    if acumulado is None:
        return None
    bronze = acumulado

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

    # Mismo arreglo que en ingest_bronze_connections (ver ese comentario para
    # el detalle completo): esta es la función que históricamente más pesaba,
    # con miles de archivos CSV pequeños de golpe.
    LOTE_TAMANO = 50
    acumulado_rta = existing
    lote_actual_rta = []

    def volcar_lote_rta():
        nonlocal acumulado_rta, lote_actual_rta
        if not lote_actual_rta:
            return
        concat_lote = pl.concat(lote_actual_rta, how='vertical_relaxed')
        acumulado_rta = concat_lote if acumulado_rta is None else pl.concat([acumulado_rta, concat_lote], how='vertical_relaxed')
        lote_actual_rta = []

    for i, f in enumerate(nuevos, 1):
        try:
            df = parse_rta_csv(f['path'], f['name'])
            lote_actual_rta.append(df)
        except Exception as e:
            print(f"  [{i}/{len(nuevos)}] {f['name']}: ERROR {e}")
        if len(lote_actual_rta) >= LOTE_TAMANO:
            volcar_lote_rta()
    volcar_lote_rta()

    if acumulado_rta is None:
        return None
    bronze = acumulado_rta

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
      1) Dedup de VERSIONES por (uuid, día, zona): si la misma zona viene
         en varios CSVs (re-exportaciones), nos quedamos con la más reciente
         (file_date mayor). Dos ZONAS distintas el mismo día son turnos reales
         y se conservan ambas para sumarlas en el Paso 2.
         La clave NO incluye métricas para que decimales distintos entre
         exportaciones no creen filas "distintas" que se sumen (bug del doble).
      2) SUMA por (uuid, día): se suman horas, viajes, km y min de todas las zonas.
         Las métricas promedio/derivadas (_avg) se RECALCULAN sobre los totales.
    """
    base = bronze_daily.filter(
        pl.col('datestr').is_not_null() & pl.col('driver_uuid').is_not_null()
    )

    # --- Paso 1: una versión por (uuid, día, zona) ---
    zona_cols = [c for c in ['city_id', 'city_name'] if c in base.columns]
    ident_keys = ['driver_uuid', 'datestr'] + zona_cols
    base = (
        base.sort(['file_date', 'num_of_trips'], descending=[True, True], nulls_last=True)
            .unique(subset=ident_keys, keep='first', maintain_order=True)
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
    Calcula, por (courier_uuid, FECHA REAL):
      - frac_horas: fracción de horas antes de las 02:00 (para la regla 02:00)
      - horas_conn: TOTAL de horas conectado del día (open+enroute+ontrip)

    Cierre de sesiones: una sesión sin end_time se cierra con el INICIO del
    siguiente evento del mismo rider (así no se pierden sus horas). Sesiones
    absurdas (>18h, error de datos) se descartan.

    horas_conn se usa como respaldo: si el online_hours del silver difiere
    mucho de las horas reales (CSV incompleto), el ajuste usa horas_conn.

    Una sesión que CRUZA las 02:00 cuenta solo la parte real antes del corte.
    """
    conn = bronze_conn.with_columns([
        pl.col('start_time').str.to_datetime(strict=False).alias('start_dt'),
        pl.col('end_time').str.to_datetime(strict=False).alias('end_dt'),
    ]).filter(pl.col('start_dt').is_not_null())

    # Cerrar sesiones sin end_time con el inicio del siguiente evento del rider
    conn = conn.sort(['courier_uuid', 'start_dt'])
    conn = conn.with_columns(
        pl.col('start_dt').shift(-1).over('courier_uuid').alias('next_start')
    )
    conn = conn.with_columns(
        pl.when(pl.col('end_dt').is_not_null()).then(pl.col('end_dt'))
          .otherwise(pl.col('next_start')).alias('end_eff')
    )

    # Solo estados de trabajo, con fin efectivo válido y posterior al inicio
    conn = conn.filter(
        pl.col('status').is_in(WORK_STATES) &
        pl.col('end_eff').is_not_null() &
        (pl.col('end_eff') > pl.col('start_dt'))
    )

    # Límite de las 02:00 del día de cada sesión (según start_dt)
    conn = conn.with_columns(
        pl.col('start_dt').dt.truncate('1d').dt.offset_by(f'{LOGICAL_DAY_CUTOFF_HOUR}h').alias('corte_02h')
    )
    conn = conn.with_columns([
        pl.col('start_dt').dt.date().alias('fecha_real'),
        ((pl.col('end_eff') - pl.col('start_dt')).dt.total_seconds() / 3600).alias('dur_h'),
        # Parte real antes de las 02:00 (solo si la sesión empieza antes del corte):
        pl.when(pl.col('start_dt') < pl.col('corte_02h'))
          .then(
              (pl.min_horizontal(pl.col('end_eff'), pl.col('corte_02h')) - pl.col('start_dt'))
              .dt.total_seconds() / 3600
          )
          .otherwise(0.0).alias('dur_madrugada'),
    ])
    # Descartar sesiones absurdas (error de datos) y madrugada negativa
    conn = conn.filter(pl.col('dur_h') <= 18)
    conn = conn.with_columns(pl.col('dur_madrugada').clip(lower_bound=0.0))

    g = (
        conn.group_by(['courier_uuid', 'fecha_real']).agg([
            pl.col('dur_h').sum().alias('horas_conn'),
            pl.col('dur_madrugada').sum().alias('horas_madrugada'),
        ])
    )
    g = g.with_columns(
        pl.when(pl.col('horas_conn') > 0)
          .then((pl.col('horas_madrugada') / pl.col('horas_conn')).clip(upper_bound=1.0))
          .otherwise(0.0).alias('frac_horas')
    )
    g = g.rename({'fecha_real': 'dia'})
    return g.select(['courier_uuid', 'dia', 'frac_horas', 'horas_conn', 'horas_madrugada'])


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
        s = s.join(rta.select(['courier_uuid', 'dia', 'frac_rta', 'rta_madrugada']),
                   left_on=['driver_uuid', '_dia'], right_on=['courier_uuid', 'dia'], how='left')
    else:
        s = s.with_columns([pl.lit(0.0).alias('frac_rta'), pl.lit(0).alias('rta_madrugada')])
    if conn is not None and len(conn) > 0:
        s = s.join(conn.select(['courier_uuid', 'dia', 'frac_horas', 'horas_conn']),
                   left_on=['driver_uuid', '_dia'], right_on=['courier_uuid', 'dia'], how='left')
    else:
        s = s.with_columns([pl.lit(0.0).alias('frac_horas'), pl.lit(None).cast(pl.Float64).alias('horas_conn')])

    s = s.with_columns([
        pl.col('frac_rta').fill_null(0.0),
        pl.col('frac_horas').fill_null(0.0),
        pl.col('rta_madrugada').fill_null(0),
    ])

    # --- Corrección de horas incompletas del silver (Opción B) ---
    # El online_hours del CSV de Uber a veces llega incompleto (el día no se
    # había consolidado al exportar). Si Connections difiere en más de 1 hora,
    # usamos las horas de Connections (que coinciden con el panel de Uber).
    # El 95% de los días coinciden, así que esto solo corrige el ~4% problemático.
    if 'horas_conn' in s.columns and 'online_hours' in s.columns:
        s = s.with_columns(
            pl.when(
                pl.col('horas_conn').is_not_null() &
                ((pl.col('horas_conn') - pl.col('online_hours')).abs() > 1.0)
            )
            .then(pl.col('horas_conn'))
            .otherwise(pl.col('online_hours'))
            .alias('online_hours')
        )

    # Columnas de PEDIDOS y de HORAS — TODO se mueve con la MISMA fracción
    # (frac_horas de Connections), para que viajes y horas viajen JUNTOS y el
    # TPH quede coherente. Si frac_horas = 0 (no hubo madrugada confirmada por
    # Connections), no se mueve NADA y el día conserva sus totales de Uber.
    PEDIDO_COLS = [c for c in ['num_of_trips', 'single_trips_total', 'accept_trips',
                               'reject_trips', 'cancel_trips', 'cancel_not_at_fault_trips',
                               'late_p2_trips', 'late_p3_trips'] if c in s.columns]
    HORA_COLS = [c for c in ['online_hours', 'active_hours', 'open_hours',
                             'enroute_p2_hours', 'ontrip_p3_hours', 'unavailable_hours',
                             'p2_km', 'p2_min', 'p3_km', 'p3_min', 'total_km', 'total_min'] if c in s.columns]

    # --- Cantidad que se mueve al día anterior ---
    # Connections es la fuente ÚNICA de la madrugada (mide horas Y actividad con
    # timestamps reales). RTA a veces no registra los pedidos de madrugada aunque
    # el rider estuviera trabajando, así que usar RTA descuadraba (movía horas
    # sin mover viajes). Con una sola fracción, viajes y horas se mueven juntos.
    #   - HORAS: proporción exacta (decimal)
    #   - PEDIDOS: proporción redondeada al entero (no hay medios viajes)
    mv_exprs = []
    for c in PEDIDO_COLS:
        mv_exprs.append((pl.col(c).fill_null(0) * pl.col('frac_horas')).round(0).alias('mv_' + c))
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

    # Marcar si la fila tuvo movimiento de madrugada (frac_horas > 0) o recibió
    # algo del día siguiente. Sirve para inspección en el dashboard.
    s = s.with_columns([
        ((pl.col('frac_horas') > 0) |
         pl.col('in_num_of_trips').is_not_null()).alias('ajustado_connections'),
        pl.col('frac_horas').alias('_frac_horas_dbg'),
        pl.col('frac_rta').alias('_frac_rta_dbg'),
    ])

    # Limpiar auxiliares
    aux = ['frac_rta', 'frac_horas', 'rta_madrugada', 'horas_conn'] + ['mv_' + c for c in PEDIDO_COLS + HORA_COLS] + \
          ['in_' + c for c in PEDIDO_COLS + HORA_COLS]
    s = s.drop([c for c in aux if c in s.columns])
    return s


# =============================================================================
# 4.5. SINCRONIZACIÓN CON SUPABASE (reemplaza a la antigua API Fleet Manager)
# =============================================================================
# Se llama UNA vez, al final de main(), después de escribir el parquet.
# No recalcula nada — solo renombra columnas y ajusta unidades para que
# coincidan con lo que espera la tabla driver_daily_stats, y sube el
# resultado con upsert (courier_uuid, day).

_RENOMBRAR_SUPABASE = {
    'driver_uuid': 'courier_uuid',
    'driver_email': 'email',
    'city_name': 'city',
    'cancel_not_at_fault_trips': 'cancel_not_at_fault',
    'tph_adj': 'tph',
}

_COLUMNAS_DESTINO_SUPABASE = [
    'day', 'courier_uuid', 'driver_name', 'driver_number', 'email', 'city',
    'flow_type', 'num_of_trips', 'online_hours', 'active_hours',
    'accept_trips', 'reject_trips', 'cancel_trips', 'cancel_not_at_fault',
    'tph', 'pct_accept', 'pct_cancel',
]


def sync_to_supabase(final: pl.DataFrame, ventana_dias: int = 21) -> None:
    """
    Sube a Supabase solo la ventana reciente (por defecto 21 dias, igual
    que REPROCESS_WEEKS*7). El UPSERT por (courier_uuid, day) cubre que
    los dias de la ventana se sobrescriban con el dato mas reciente.
    """
    df = final.clone()

    if '_dia' in df.columns:
        df = df.rename({'_dia': 'day'})
    elif 'datestr' in df.columns:
        df = df.with_columns(pl.col('datestr').dt.date().alias('day'))

    for col_faltante in ('pct_aceptacion', 'pct_cancelacion', 'tph_adj', 'flow_type'):
        if col_faltante not in df.columns:
            df = df.with_columns(pl.lit(0.0 if col_faltante != 'flow_type' else None).alias(col_faltante))

    df = df.rename({k: v for k, v in _RENOMBRAR_SUPABASE.items() if k in df.columns})

    df = df.with_columns([
        (pl.col('pct_aceptacion') / 100.0).alias('pct_accept'),
        (pl.col('pct_cancelacion') / 100.0).alias('pct_cancel'),
    ])

    for col in _COLUMNAS_DESTINO_SUPABASE:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    df = df.select(_COLUMNAS_DESTINO_SUPABASE)

    if 'day' in df.columns:
        limite = date.today() - timedelta(days=ventana_dias)
        df = df.filter(pl.col('day') >= limite)

    registros = df.with_columns(pl.col('day').cast(pl.Utf8)).to_dicts()
    if not registros:
        print('[sync_to_supabase] Nada que sincronizar en la ventana reciente.')
        return

    supabase = create_client(os.environ['SUPABASE_METRICS_URL'], os.environ['SUPABASE_METRICS_SERVICE_KEY'])
    TAMANO_LOTE = 500
    subidos = 0
    for i in range(0, len(registros), TAMANO_LOTE):
        lote = registros[i:i + TAMANO_LOTE]
        supabase.table('driver_daily_stats').upsert(lote, on_conflict='courier_uuid,day').execute()
        subidos += len(lote)
        print(f'[sync_to_supabase] {subidos}/{len(registros)} filas sincronizadas')

    print(f'[sync_to_supabase] {subidos} filas sincronizadas a Supabase (ventana: ultimos {ventana_dias} dias)')


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
    print("\n--- Construyendo silver ---")
    # Diagnóstico: cuántas filas del bronce, cuántas tras dedup+suma
    n_bronze = len(bronze_daily)
    silver = build_silver(bronze_daily)
    print(f"[silver] Bronze daily: {n_bronze:,} filas → silver: {len(silver):,} filas (rider+día únicos)")
    # Aviso si quedan duplicados (no debería)
    _dup = (silver.with_columns(pl.col('datestr').dt.date().alias('_d'))
                  .group_by(['driver_uuid', '_d']).agg(pl.len().alias('n'))
                  .filter(pl.col('n') > 1))
    if len(_dup) > 0:
        print(f"[silver] ⚠ ADVERTENCIA: {len(_dup)} (rider,día) con más de una fila tras el silver")

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

    # --- Sincronizar el resultado a Supabase (reemplaza a la API Fleet Manager) ---
    sync_to_supabase(final)

    # NOTA: el pipeline NO genera dashboard.html. El dashboard de producción
    # es la app React (Closer CRM), que lee las métricas desde Supabase,
    # no del parquet directamente.

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

