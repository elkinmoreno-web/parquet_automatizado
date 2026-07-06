# -*- coding: utf-8 -*-
"""
Pipeline Closer Logistics — Viajes + Connections con regla de las 02:00
=======================================================================
VERSIÓN: v2.1-fix-markets  (2026-07-06)

CAMBIOS vs v2.0 (los dos fixes están marcados con "FIX v2.1"):

  FIX 1 — build_silver (EL BUG DE LOS VIAJES QUE FALTABAN):
    Uber puede traer VARIAS filas para el mismo rider y el mismo día con la
    MISMA ciudad (city_name=MADRID) pero distinto market_name (p. ej.
    MADRID CENTRO + MADRID VALLECAS = dos turnos reales del mismo día).
    El dedup viejo usaba la clave (uuid, día, city_id, city_name), veía esas
    dos filas como "versiones duplicadas" y BORRABA una → se perdían viajes.
    (Caso real: Jose Gregorio 01/07 → CENTRO 8 viajes + VALLECAS 11 = 19,
    pero el silver se quedaba solo con 11.)
    Las horas parecían bien porque la corrección con Connections las
    "rescataba", pero los viajes no tienen rescate → dashboard con menos pedidos.

    Solución: para cada (rider, día) nos quedamos con TODAS las filas del
    ARCHIVO MÁS RECIENTE que contenga ese (rider, día), y las sumamos.
    Cada archivo es una "foto" completa del día → la foto más nueva manda.
    Así da igual si Uber parte el día por market, city, form_factor o lo
    que sea: se suma todo lo que venga en la foto más reciente.

  FIX 2 — apply_adjustment:
    Si un rider trabajó SOLO de madrugada el día D (y no trabajó el día D-1),
    lo movido a D-1 caía en un día sin fila en el silver y se PERDÍA
    (el join era 'left'). Ahora el join es 'full': si D-1 no existe, se crea
    la fila y los viajes/horas movidos no se pierden.

El resto es idéntico a v2.0.
"""

PIPELINE_VERSION = "v2.1-fix-markets"

import os
import re
import glob
from datetime import datetime, timedelta
import polars as pl

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

COURIER_DAILY_DIR = os.environ.get('COURIER_DAILY_DIR', 'COURIER_DAILY')
CONNECTIONS_DIR   = os.environ.get('CONNECTIONS_DIR', 'CONNECTIONS')
RTA_DIR           = os.environ.get('RTA_DIR', 'CANCELLATIONS_RTA')

OUTPUT_DIR = os.environ.get('OUTPUT_DIR', 'datos_salida')
SILVER_NAME = os.environ.get('SILVER_NAME', 'rides_silver')
BRONZE_SUFFIX = os.environ.get('BRONZE_SUFFIX', '')

BRONZE_DAILY_PARQUET = os.path.join(OUTPUT_DIR, 'bronze_daily' + BRONZE_SUFFIX + '.parquet')
BRONZE_CONN_PARQUET  = os.path.join(OUTPUT_DIR, 'bronze_connections' + BRONZE_SUFFIX + '.parquet')
BRONZE_RTA_PARQUET   = os.path.join(OUTPUT_DIR, 'bronze_rta' + BRONZE_SUFFIX + '.parquet')

SILVER_PARQUET = os.path.join(OUTPUT_DIR, SILVER_NAME + '.parquet')

REPROCESS_WEEKS = 3
LOGICAL_DAY_CUTOFF_HOUR = 2

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
# 1. BRONZE — COURIER_DAILY (incremental)  [SIN CAMBIOS]
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
    processed = set()
    existing = None
    if os.path.exists(BRONZE_DAILY_PARQUET):
        existing = pl.read_parquet(BRONZE_DAILY_PARQUET)
        processed = set(existing['file_name'].unique().to_list())
        print(f"[daily] Bronze previo: {len(existing):,} filas, {len(processed)} archivos ya procesados")
    else:
        print("[daily] Sin bronze previo (primera ejecución)")

    files = []
    for path in glob.glob(os.path.join(COURIER_DAILY_DIR, '*.csv')):
        name = os.path.basename(path)
        if not DAILY_PATTERN.search(name):
            continue
        files.append({'path': path, 'name': name, 'ts': extract_ts(name) or datetime.min})

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
# 2. BRONZE — CONNECTIONS (incremental)  [SIN CAMBIOS]
# =============================================================================

CONN_COLS = ['courier_uuid', 'courier_name', 'contact_number', 'fleet_name', 'status',
             'datestr', 'start_time', 'end_time', 'job_daily_rank']


def parse_conn_csv(filepath, file_name):
    sep = detect_sep(filepath)
    df = pl.read_csv(
        filepath, separator=sep, infer_schema=False,
        null_values=['', 'NA', 'null', 'NULL', '\\N'],
        truncate_ragged_lines=True,
    )
    keep = [c for c in CONN_COLS if c in df.columns]
    df = df.select(keep)
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

    before = len(bronze)
    bronze = bronze.unique(subset=['courier_uuid', 'start_time', 'status'], keep='first')
    print(f"[conn] Dedup: {before:,} → {len(bronze):,} filas")

    bronze.write_parquet(BRONZE_CONN_PARQUET, compression='zstd')
    print(f"[conn] Bronze guardado: {len(bronze):,} filas")
    return bronze


# =============================================================================
# 2b. BRONZE — CANCELLATIONS_RTA (incremental)  [SIN CAMBIOS]
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

    before = len(bronze)
    bronze = bronze.unique(subset=['offer_id'], keep='first')
    print(f"[rta] Dedup por offer_id: {before:,} → {len(bronze):,} filas")

    bronze.write_parquet(BRONZE_RTA_PARQUET, compression='zstd')
    print(f"[rta] Bronze guardado: {len(bronze):,} filas")
    return bronze


# =============================================================================
# 3. RTA — fracción de pedidos de madrugada  [SIN CAMBIOS]
# =============================================================================

def reconstruct_rta(bronze_rta):
    """
    Calcula, por (courier_uuid, FECHA REAL), la FRACCIÓN de pedidos que ocurrió
    antes de las 02:00 (madrugada). Solo informativo/debug.
    """
    r = bronze_rta.with_columns(
        pl.col('timestamp').str.to_datetime(strict=False).alias('ts')
    ).filter(pl.col('ts').is_not_null())

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
# SILVER  ★★★ FIX v2.1 — AQUÍ ESTABA EL BUG DE LOS VIAJES PERDIDOS ★★★
# =============================================================================

def build_silver(bronze_daily):
    """
    Construye el silver con UNA fila por (driver_uuid, día), sumando TODOS los
    turnos del mismo día (markets/zonas distintas, p. ej. CENTRO + VALLECAS).

    ★ FIX v2.1 — Cómo se eligen las filas correctas:

      Cada archivo CSV de Uber es una "FOTO" completa de cada (rider, día):
      puede traer 1 fila o VARIAS (una por market/zona en que trabajó).
      Un archivo más nuevo trae la foto más consolidada de ese día.

      Por eso, para cada (rider, día):
        1) Buscamos el archivo MÁS RECIENTE que contenga ese (rider, día).
        2) Nos quedamos con TODAS sus filas de ese archivo (la foto completa).
        3) Las sumamos.

      El dedup viejo usaba la clave (uuid, día, city_id, city_name) y por eso
      dos markets del mismo día con la MISMA ciudad (MADRID CENTRO y
      MADRID VALLECAS → ambos city_name=MADRID) parecían "duplicados" y se
      borraba uno → viajes perdidos. Con la lógica de "foto más reciente
      completa" da igual en qué columna venga partido el día: se suma todo.
    """
    base = bronze_daily.filter(
        pl.col('datestr').is_not_null() & pl.col('driver_uuid').is_not_null()
    )

    # --- Paso 1 (FIX v2.1): quedarnos con la FOTO más reciente de cada (uuid, día) ---
    base = (
        base.with_columns(
            pl.col('file_date').max().over(['driver_uuid', 'datestr']).alias('_max_fd')
        )
        .filter(pl.col('file_date') == pl.col('_max_fd'))
        .drop('_max_fd')
    )
    # Seguridad: si el MISMO export se guardó dos veces con nombres distintos
    # (mismo timestamp en el nombre), quitamos filas 100% idénticas en datos.
    canon_present = [c for c in CANONICAL_DAILY if c in base.columns]
    base = base.unique(subset=canon_present, keep='first')

    # --- Paso 2: sumar todas las filas (markets/zonas) del mismo (uuid, día) ---
    SUM_COLS = [c for c in [
        'online_hours', 'active_hours', 'open_hours',
        'enroute_p2_hours', 'ontrip_p3_hours', 'unavailable_hours',
        'num_of_trips', 'single_trips_total', 'late_p2_trips', 'late_p3_trips',
        'accept_trips', 'reject_trips', 'cancel_trips', 'cancel_not_at_fault_trips',
        'p2_km', 'p2_min', 'p3_km', 'p3_min', 'total_km', 'total_min',
    ] if c in base.columns]

    # Texto/identidad: tomamos la del turno con más viajes (venimos ordenados así)
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

    # Recorte a la ventana reciente
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

    ★ FIX v2.1 — Sesiones que CRUZAN la medianoche:
      Antes, una sesión 23:32 → 00:10 se anclaba entera al día en que EMPEZABA,
      y como empezaba después de las 02:00 su "madrugada" era 0. Resultado:
      los minutos después de medianoche (00:00–02:00) nunca se detectaban como
      madrugada del día siguiente y NO se movían al día anterior (el CSV de
      Uber sí los apunta al día siguiente). Ahora cada sesión se PARTE en la
      medianoche: el trozo de después de las 00:00 cuenta como día siguiente
      y su parte antes de las 02:00 sí se detecta como madrugada. Así el
      cálculo por día calendario queda alineado con cómo cuenta el CSV, y la
      regla de las 02:00 lo devuelve al día anterior.
    """
    conn = bronze_conn.with_columns([
        pl.col('start_time').str.to_datetime(strict=False).alias('start_dt'),
        pl.col('end_time').str.to_datetime(strict=False).alias('end_dt'),
    ]).filter(pl.col('start_dt').is_not_null())

    conn = conn.sort(['courier_uuid', 'start_dt'])
    conn = conn.with_columns(
        pl.col('start_dt').shift(-1).over('courier_uuid').alias('next_start')
    )
    conn = conn.with_columns(
        pl.when(pl.col('end_dt').is_not_null()).then(pl.col('end_dt'))
          .otherwise(pl.col('next_start')).alias('end_eff')
    )

    conn = conn.filter(
        pl.col('status').is_in(WORK_STATES) &
        pl.col('end_eff').is_not_null() &
        (pl.col('end_eff') > pl.col('start_dt'))
    )
    # Descartar sesiones absurdas ANTES de partir (error de datos)
    conn = conn.with_columns(
        ((pl.col('end_eff') - pl.col('start_dt')).dt.total_seconds() / 3600).alias('dur_total_h')
    ).filter(pl.col('dur_total_h') <= 18)

    # --- ★ FIX v2.1: partir cada sesión en la medianoche ---
    # medianoche = las 00:00 del día siguiente al inicio de la sesión.
    # Con sesiones de ≤18h, como mucho cruzan UNA medianoche.
    medianoche = pl.col('start_dt').dt.truncate('1d').dt.offset_by('1d')
    # Trozo 1: desde el inicio hasta la medianoche (o el fin, lo que llegue antes)
    p1 = conn.with_columns([
        pl.col('start_dt').alias('seg_s'),
        pl.min_horizontal(pl.col('end_eff'), medianoche).alias('seg_e'),
    ])
    # Trozo 2: desde la medianoche hasta el fin (solo si la sesión la cruza)
    p2 = conn.filter(pl.col('end_eff') > medianoche).with_columns([
        medianoche.alias('seg_s'),
        pl.col('end_eff').alias('seg_e'),
    ])
    segs = pl.concat([
        p1.select(['courier_uuid', 'seg_s', 'seg_e']),
        p2.select(['courier_uuid', 'seg_s', 'seg_e']),
    ]).filter(pl.col('seg_e') > pl.col('seg_s'))

    # Cada trozo pertenece al día calendario en que ocurre; su madrugada es
    # lo que caiga entre las 00:00 y las 02:00 de ESE día.
    corte_02h = pl.col('seg_s').dt.truncate('1d').dt.offset_by(f'{LOGICAL_DAY_CUTOFF_HOUR}h')
    segs = segs.with_columns([
        pl.col('seg_s').dt.date().alias('fecha_real'),
        ((pl.col('seg_e') - pl.col('seg_s')).dt.total_seconds() / 3600).alias('dur_h'),
        pl.when(pl.col('seg_s') < corte_02h)
          .then(
              (pl.min_horizontal(pl.col('seg_e'), corte_02h) - pl.col('seg_s'))
              .dt.total_seconds() / 3600
          )
          .otherwise(0.0).alias('dur_madrugada'),
    ])
    segs = segs.with_columns(pl.col('dur_madrugada').clip(lower_bound=0.0))

    g = (
        segs.group_by(['courier_uuid', 'fecha_real']).agg([
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
    Modelo PROPORCIONAL sobre el SILVER (regla 02:00). [Lógica igual que v2.0]

    ★ FIX v2.1: lo que se mueve a D-1 ya no se pierde si el rider no tiene
    fila en D-1 (join 'full' en lugar de 'left' + creación de la fila).
    """
    silver = silver.with_columns(pl.col('datestr').dt.date().alias('_dia'))

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

    PEDIDO_COLS = [c for c in ['num_of_trips', 'single_trips_total', 'accept_trips',
                               'reject_trips', 'cancel_trips', 'cancel_not_at_fault_trips',
                               'late_p2_trips', 'late_p3_trips'] if c in s.columns]
    HORA_COLS = [c for c in ['online_hours', 'active_hours', 'open_hours',
                             'enroute_p2_hours', 'ontrip_p3_hours', 'unavailable_hours',
                             'p2_km', 'p2_min', 'p3_km', 'p3_min', 'total_km', 'total_min'] if c in s.columns]
    MOVE_COLS = PEDIDO_COLS + HORA_COLS

    mv_exprs = []
    for c in PEDIDO_COLS:
        mv_exprs.append((pl.col(c).fill_null(0) * pl.col('frac_horas')).round(0).alias('mv_' + c))
    for c in HORA_COLS:
        mv_exprs.append((pl.col(c).fill_null(0) * pl.col('frac_horas')).alias('mv_' + c))
    s = s.with_columns(mv_exprs)

    # --- Lo que entra en D-1 ---
    movidos = s.select(
        ['driver_uuid'] +
        [(pl.col('_dia') - pl.duration(days=1)).alias('_dia')] +
        [pl.col('mv_' + c).alias('in_' + c) for c in MOVE_COLS]
    )
    movidos = movidos.group_by(['driver_uuid', '_dia']).agg(
        [pl.col('in_' + c).sum() for c in MOVE_COLS]
    )
    # No arrastrar movimientos vacíos (evita crear filas fantasma en el full join)
    movidos = movidos.filter(
        pl.sum_horizontal([pl.col('in_' + c).abs() for c in MOVE_COLS]) > 0
    )

    # --- Restar de cada día lo que sale ---
    s = s.with_columns(
        [(pl.col(c).fill_null(0) - pl.col('mv_' + c)).alias(c) for c in MOVE_COLS]
    )

    # --- ★ FIX v2.1: unir lo que entra con join FULL (antes 'left') ---
    # Con 'left', si el rider no tenía fila en D-1 (p. ej. SOLO trabajó de
    # madrugada el día D), los viajes/horas movidos desaparecían.
    # Con 'full' + coalesce, esa fila se crea y no se pierde nada.
    s = s.join(movidos, on=['driver_uuid', '_dia'], how='full', coalesce=True)

    # Filas nuevas (solo entrantes): métricas a 0 antes de sumar lo que entra
    s = s.with_columns([pl.col(c).fill_null(0.0) for c in MOVE_COLS])
    s = s.with_columns(
        [(pl.col(c) + pl.col('in_' + c).fill_null(0)).alias(c) for c in MOVE_COLS]
    )
    # Completar datestr y fracciones en las filas creadas por el full join
    s = s.with_columns([
        pl.when(pl.col('datestr').is_null())
          .then(pl.col('_dia').cast(pl.Datetime('us')))
          .otherwise(pl.col('datestr')).alias('datestr'),
        pl.col('frac_horas').fill_null(0.0),
        pl.col('frac_rta').fill_null(0.0),
    ])
    # Rellenar metadatos del rider (nombre, email...) en las filas creadas,
    # copiándolos de otra fila del mismo rider.
    META_COLS = [c for c in ['driver_name', 'driver_number', 'driver_email',
                             'fleet_name', 'city_id', 'city_name', 'market_name',
                             'form_factor', 'weekstr'] if c in s.columns]
    s = s.with_columns([
        pl.col(c).fill_null(pl.col(c).max().over('driver_uuid')) for c in META_COLS
    ])

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

    s = s.with_columns([
        ((pl.col('frac_horas') > 0) |
         pl.col('in_num_of_trips').is_not_null()).alias('ajustado_connections'),
        pl.col('frac_horas').alias('_frac_horas_dbg'),
        pl.col('frac_rta').alias('_frac_rta_dbg'),
    ])

    aux = ['frac_rta', 'frac_horas', 'rta_madrugada', 'horas_conn'] + \
          ['mv_' + c for c in MOVE_COLS] + ['in_' + c for c in MOVE_COLS]
    s = s.drop([c for c in aux if c in s.columns])
    return s


# =============================================================================
# 5. MAIN  [SIN CAMBIOS salvo versión]
# =============================================================================

def main():
    bronze_daily = ingest_bronze_daily()
    if bronze_daily is None:
        print("\n✗ No hay datos de COURIER_DAILY. Abortando.")
        return
    bronze_conn = ingest_bronze_connections()
    bronze_rta  = ingest_bronze_rta()

    print("\n--- Construyendo silver ---")
    n_bronze = len(bronze_daily)
    silver = build_silver(bronze_daily)
    print(f"[silver] Bronze daily: {n_bronze:,} filas → silver: {len(silver):,} filas (rider+día únicos)")
    _dup = (silver.with_columns(pl.col('datestr').dt.date().alias('_d'))
                  .group_by(['driver_uuid', '_d']).agg(pl.len().alias('n'))
                  .filter(pl.col('n') > 1))
    if len(_dup) > 0:
        print(f"[silver] ⚠ ADVERTENCIA: {len(_dup)} (rider,día) con más de una fila tras el silver")

    recon_conn = None
    if bronze_conn is not None and len(bronze_conn) > 0:
        recon_conn = reconstruct_connections(bronze_conn)
        print(f"[conn] Reconstrucción: {len(recon_conn):,} combinaciones (rider, día)")

    recon_rta = None
    if bronze_rta is not None and len(bronze_rta) > 0:
        recon_rta = reconstruct_rta(bronze_rta)
        print(f"[rta] Reconstrucción: {len(recon_rta):,} combinaciones (rider, día)")

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

    antes = len(final)
    final = final.filter(
        ~((pl.col('num_of_trips').fill_null(0) == 0) &
          (pl.col('online_hours').fill_null(0) == 0))
    )
    quitadas = antes - len(final)
    if quitadas > 0:
        print(f"[limpieza] Filas vacías eliminadas (0 viajes y 0 horas): {quitadas}")

    final.write_parquet(SILVER_PARQUET, compression='zstd')
    print(f"\n✓ Parquet final: {SILVER_PARQUET} ({len(final):,} filas)")

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
