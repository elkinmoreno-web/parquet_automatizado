# -*- coding: utf-8 -*-
"""
Pipeline Closer Logistics — Viajes + Connections con regla de las 02:00
=======================================================================
VERSIÓN: v2.0-connections-only  (2026-06-02)

  TODO se calcula desde Connections (viajes, horas, aceptados, cancelados,
  % aceptación, % cancelación). El silver solo aporta metadatos del rider.
  Días sin cobertura de Connections se descartan.
"""

PIPELINE_VERSION = "v4.0-rta-connections-silver"

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
SILVER_CSV     = os.path.join(OUTPUT_DIR, SILVER_NAME + '.csv')
DASHBOARD_HTML = os.path.join(OUTPUT_DIR, 'dashboard.html')

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
    Cuenta pedidos por (courier_uuid, DÍA LÓGICO) desde CANCELLATIONS_RTA.
    Cada offer_id es un pedido individual (resuelve los dobles/triples).
      - rta_viajes = ACCEPT - CANCEL  (pedidos que REALMENTE hizo)
      - rta_cancel = nº de CANCEL
      - REJECT no cuenta
    Validado contra el silver: ACCEPT-CANCEL cuadra al ~93% (dif <=1 por día).
    La regla de las 02:00 se aplica sobre el timestamp de cada pedido:
    un pedido a las 00:30 del día D cuenta para el día D-1.
    """
    r = bronze_rta.with_columns(
        pl.col('timestamp').str.to_datetime(strict=False).alias('ts')
    ).filter(pl.col('ts').is_not_null())

    # Día lógico por timestamp
    r = r.with_columns(
        pl.when(pl.col('ts').dt.hour() < LOGICAL_DAY_CUTOFF_HOUR)
          .then(pl.col('ts').dt.date() - pl.duration(days=1))
          .otherwise(pl.col('ts').dt.date())
          .alias('dia')
    )

    out = (
        r.group_by(['courier_uuid', 'dia']).agg([
            (pl.col('courier_action') == 'ACCEPT').sum().alias('rta_accept'),
            (pl.col('courier_action') == 'CANCEL').sum().alias('rta_cancel'),
        ])
    )
    # Viajes REALES = aceptados - cancelados (pedidos que realmente hizo)
    out = out.with_columns(
        (pl.col('rta_accept') - pl.col('rta_cancel')).clip(lower_bound=0).alias('rta_viajes')
    )
    return out


# =============================================================================
# SILVER
# =============================================================================

def build_silver(bronze_daily):
    # Dedup por (driver_uuid, datestr). Criterio:
    #   1) file_date más reciente (la última versión del dato)
    #   2) si empatan en file_date (mismo timestamp en el nombre de dos archivos),
    #      quedarse con la de mayor num_of_trips (la versión más completa)
    #   3) garantía final: una sola fila por (uuid, día) con unique()
    silver = (
        bronze_daily
        .filter(pl.col('datestr').is_not_null() & pl.col('driver_uuid').is_not_null())
        .sort(
            ['driver_uuid', 'datestr', 'file_date', 'num_of_trips'],
            descending=[False, False, True, True],
            nulls_last=True,
        )
        .unique(subset=['driver_uuid', 'datestr'], keep='first', maintain_order=True)
    )

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
    Cuenta por (courier_uuid, DÍA LÓGICO) desde CONNECTIONS:
      - conn_viajes = pares enroute→ontrip (anclados al día lógico del enroute)
      - conn_cancel = enroute sin ontrip detrás (aceptó pero no completó)
      - conn_horas  = suma duración de open+enroute+ontrip (end válido)
    La regla 02:00 se aplica sobre start_time de cada fila.
    NOTA: Connections subcuenta viajes con pedidos dobles/triples (un ontrip
    puede llevar varios pedidos). Por eso RTA es preferente; esto es respaldo.
    """
    conn = bronze_conn.with_columns([
        pl.col('start_time').str.to_datetime(strict=False).alias('start_dt'),
        pl.col('end_time').str.to_datetime(strict=False).alias('end_dt'),
    ]).filter(pl.col('start_dt').is_not_null())

    # Día lógico por start_dt (regla 02:00)
    conn = conn.with_columns(
        pl.when(pl.col('start_dt').dt.hour() < LOGICAL_DAY_CUTOFF_HOUR)
          .then(pl.col('start_dt').dt.date() - pl.duration(days=1))
          .otherwise(pl.col('start_dt').dt.date())
          .alias('dia')
    )

    conn = conn.sort(['courier_uuid', 'start_dt'])
    conn = conn.with_columns(
        pl.col('status').shift(-1).over('courier_uuid').alias('next_status')
    )

    # Enroute = pedido aceptado. Completado si la siguiente fila es ontrip.
    enroutes = conn.filter(pl.col('status') == 'enroute').with_columns(
        (pl.col('next_status') == 'ontrip').alias('completado')
    )
    viajes = (
        enroutes.group_by(['courier_uuid', 'dia']).agg([
            pl.col('completado').sum().alias('conn_viajes'),
            (~pl.col('completado')).sum().alias('conn_cancel'),
        ])
    )

    # Horas: open+enroute+ontrip con end válido
    horas = (
        conn.filter(pl.col('status').is_in(WORK_STATES) & pl.col('end_dt').is_not_null())
            .with_columns(((pl.col('end_dt') - pl.col('start_dt')).dt.total_seconds() / 3600).alias('dur_h'))
            .group_by(['courier_uuid', 'dia'])
            .agg(pl.col('dur_h').sum().alias('conn_horas'))
    )

    recon = viajes.join(horas, on=['courier_uuid', 'dia'], how='full', coalesce=True)
    recon = recon.with_columns([
        pl.col('conn_viajes').fill_null(0),
        pl.col('conn_cancel').fill_null(0),
        pl.col('conn_horas').fill_null(0.0),
    ])
    return recon


def apply_adjustment(silver, rta, conn):
    """
    Modelo de 3 FUENTES con día lógico directo (regla 02:00 por timestamp).

    Jerarquía de PEDIDOS/VIAJES por (rider, día lógico):
      1) RTA (CANCELLATIONS_RTA): viajes = ACCEPT, cancelados = CANCEL.
         Es la fuente correcta (cuenta cada pedido, incl. dobles/triples).
      2) Si no hay RTA ese día pero sí CONNECTIONS: respaldo temporal con
         los pares enroute→ontrip de Connections (subcuenta, pero es lo que hay).
      3) Si no hay ninguna de las dos: el día no entra (se descarta).

    HORAS: siempre de CONNECTIONS (open+enroute+ontrip).
    TPH: viajes / horas.
    % Aceptación = viajes / (viajes + cancelados) * 100
    % Cancelación = cancelados / (viajes + cancelados) * 100

    Metadatos del rider (nombre, móvil, ciudad...): del silver.
    Se añaden columnas de DEBUG para comparar contra el silver:
      silver_viajes, dif_vs_silver, fuente.
    """
    silver = silver.with_columns(pl.col('datestr').dt.date().alias('_dia'))

    # --- Tabla de actividad unificada por (uuid, día lógico) ---
    if rta is not None and len(rta) > 0:
        act = rta.join(conn, on=['courier_uuid', 'dia'], how='full', coalesce=True)
    else:
        # Sin RTA: solo Connections
        act = conn.with_columns([
            pl.lit(None).cast(pl.Int64).alias('rta_viajes'),
            pl.lit(None).cast(pl.Int64).alias('rta_cancel'),
        ]) if conn is not None else None

    if act is None or len(act) == 0:
        # No hay ni RTA ni Connections: devolver silver tal cual (sin ajuste)
        return silver.with_columns([
            pl.when(pl.col('online_hours') > 0)
              .then(pl.col('num_of_trips') / pl.col('online_hours'))
              .otherwise(0.0).alias('tph_adj'),
            pl.lit('silver').alias('fuente'),
            pl.lit(False).alias('ajustado_connections'),
            pl.col('num_of_trips').alias('silver_viajes'),
            pl.lit(0.0).alias('dif_vs_silver'),
            pl.lit(0.0).alias('pct_aceptacion'),
            pl.lit(0.0).alias('pct_cancelacion'),
        ]).drop('_dia')

    # Asegurar columnas de ambas fuentes
    for c, t in [('rta_viajes', pl.Int64), ('rta_cancel', pl.Int64),
                 ('conn_viajes', pl.Int64), ('conn_cancel', pl.Int64), ('conn_horas', pl.Float64)]:
        if c not in act.columns:
            act = act.with_columns(pl.lit(None).cast(t).alias(c))

    tiene_rta = pl.col('rta_viajes').is_not_null()

    # Viajes/cancelados: RTA preferente, Connections de respaldo
    act = act.with_columns([
        pl.when(tiene_rta).then(pl.col('rta_viajes'))
          .otherwise(pl.col('conn_viajes').fill_null(0)).alias('num_of_trips'),
        pl.when(tiene_rta).then(pl.col('rta_cancel').fill_null(0))
          .otherwise(pl.col('conn_cancel').fill_null(0)).alias('cancel_trips'),
        pl.col('conn_horas').fill_null(0.0).alias('online_hours'),
        pl.when(tiene_rta).then(pl.lit('rta'))
          .otherwise(pl.lit('connections')).alias('fuente'),
    ])
    # accept_trips = viajes (cada pedido aceptado). aceptados = ACCEPT.
    act = act.with_columns([
        pl.col('num_of_trips').cast(pl.Float64).alias('num_of_trips'),
        pl.col('cancel_trips').cast(pl.Float64).alias('cancel_trips'),
        pl.col('num_of_trips').cast(pl.Float64).alias('accept_trips'),
    ])

    # TPH y porcentajes
    act = act.with_columns([
        pl.when(pl.col('online_hours') > 0)
          .then(pl.col('num_of_trips') / pl.col('online_hours'))
          .otherwise(0.0).alias('tph_adj'),
        pl.when((pl.col('num_of_trips') + pl.col('cancel_trips')) > 0)
          .then(pl.col('num_of_trips') / (pl.col('num_of_trips') + pl.col('cancel_trips')) * 100)
          .otherwise(0.0).alias('pct_aceptacion'),
        pl.when((pl.col('num_of_trips') + pl.col('cancel_trips')) > 0)
          .then(pl.col('cancel_trips') / (pl.col('num_of_trips') + pl.col('cancel_trips')) * 100)
          .otherwise(0.0).alias('pct_cancelacion'),
        pl.lit(True).alias('ajustado_connections'),
        pl.col('dia').cast(pl.Datetime).alias('datestr'),
        pl.col('courier_uuid').alias('driver_uuid'),
    ])

    # --- Metadatos del rider desde el silver (fila más reciente por rider) ---
    meta_cols = ['driver_uuid', 'driver_name', 'driver_number', 'driver_email',
                 'fleet_name', 'city_id', 'city_name', 'market_name', 'form_factor', 'weekstr']
    meta_cols = [c for c in meta_cols if c in silver.columns]
    rider_meta = (
        silver.sort('_dia', descending=True)
        .group_by('driver_uuid').first()
        .select(meta_cols)
    )
    act = act.join(rider_meta, on='driver_uuid', how='left')

    # --- DEBUG: traer silver_viajes del mismo (uuid, día) para comparar ---
    silver_cmp = silver.select([
        'driver_uuid', '_dia',
        pl.col('num_of_trips').alias('silver_viajes'),
    ])
    act = act.join(silver_cmp, left_on=['driver_uuid', 'dia'],
                   right_on=['driver_uuid', '_dia'], how='left')
    act = act.with_columns([
        pl.col('silver_viajes').fill_null(0.0),
        (pl.col('num_of_trips') - pl.col('silver_viajes').fill_null(0)).alias('dif_vs_silver'),
    ])

    # Limpiar columnas auxiliares de fuentes
    aux = ['rta_viajes', 'rta_cancel', 'conn_viajes', 'conn_cancel', 'conn_horas', 'dia']
    act = act.drop([c for c in aux if c in act.columns])
    return act


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

    # --- Ajuste con 3 fuentes ---
    if recon_conn is not None or recon_rta is not None:
        final = apply_adjustment(silver, recon_rta, recon_conn)
        # Resumen de fuentes usadas
        if 'fuente' in final.columns:
            print("[ajuste] Filas por fuente:")
            for r in final.group_by('fuente').agg(pl.len().alias('n')).sort('n', descending=True).iter_rows(named=True):
                print(f"         {r['fuente']}: {r['n']:,}")
    else:
        print("[ajuste] Sin Connections ni RTA — silver sin ajustar")
        final = silver.with_columns([
            (pl.col('num_of_trips') / pl.col('online_hours')).alias('tph_adj'),
            pl.lit('silver').alias('fuente'),
            pl.lit(False).alias('ajustado_connections'),
            pl.col('num_of_trips').alias('silver_viajes'),
            pl.lit(0.0).alias('dif_vs_silver'),
        ]).drop('_dia')

    # --- Quitar filas de días vacíos (0 viajes Y 0 horas) ---
    antes = len(final)
    final = final.filter(
        ~((pl.col('num_of_trips').fill_null(0) == 0) &
          (pl.col('online_hours').fill_null(0) == 0))
    )
    quitadas = antes - len(final)
    if quitadas > 0:
        print(f"[limpieza] Filas vacías eliminadas (0 viajes y 0 horas): {quitadas}")

    # --- Salidas ---
    final.write_parquet(SILVER_PARQUET, compression='zstd')
    final.write_csv(SILVER_CSV)
    print(f"\n✓ Parquet final: {SILVER_PARQUET} ({len(final):,} filas)")
    print(f"✓ CSV inspección: {SILVER_CSV}")

    generate_dashboard(final)
    print(f"✓ Dashboard: {DASHBOARD_HTML}")

    # --- Resumen de verificación: Edgar (637794903) por consola ---
    try:
        ed = final.filter(
            pl.col('driver_number').cast(pl.Utf8).str.contains('637794903')
        ).sort('datestr')
        if len(ed) > 0:
            print("\n" + "=" * 64)
            print(f"VERIFICACIÓN — Edgar (637794903) — generado por {PIPELINE_VERSION}")
            print("=" * 64)
            chk = ed.select([
                pl.col('datestr').dt.date().alias('dia'),
                pl.col('num_of_trips').round(0).alias('viajes'),
                pl.col('silver_viajes').round(0).alias('silver'),
                pl.col('dif_vs_silver').round(0).alias('dif'),
                pl.col('online_hours').round(2).alias('horas'),
                pl.col('tph_adj').round(2).alias('tph'),
                pl.col('cancel_trips').round(0).alias('canc'),
                pl.col('pct_aceptacion').round(0).alias('%ac'),
                pl.col('fuente').alias('fuente'),
            ])
            with pl.Config(tbl_rows=40, fmt_str_lengths=20):
                print(chk)
            # Chequeo del duplicado
            dups = (ed.with_columns(pl.col('datestr').dt.date().alias('d'))
                      .group_by('d').agg(pl.len().alias('n')).filter(pl.col('n') > 1))
            if len(dups) > 0:
                print(f"⚠ AÚN HAY DUPLICADOS: {dups['d'].to_list()}")
            else:
                print("✓ Sin duplicados (1 fila por día)")
    except Exception as e:
        print(f"(No se pudo imprimir el resumen de Edgar: {e})")

    print("\n¡Proceso completado!")


def generate_dashboard(df):
    """Dashboard HTML mínimo para inspección visual de los datos."""
    # Preparar datos resumidos por rider/día
    insp = df.select([
        pl.col('datestr').dt.strftime('%Y-%m-%d').alias('dia'),
        pl.col('driver_name').alias('rider'),
        pl.col('driver_number').alias('movil'),
        pl.col('city_name').alias('ciudad'),
        pl.col('num_of_trips').round(0).alias('viajes'),
        pl.col('online_hours').round(2).alias('horas'),
        pl.col('tph_adj').round(2).alias('tph'),
        pl.col('accept_trips').round(0).alias('aceptados'),
        pl.col('pct_aceptacion').round(0).alias('pct_acept'),
        pl.col('pct_cancelacion').round(0).alias('pct_canc'),
        pl.col('ajustado_connections').alias('ajustado'),
    ]).sort(['dia', 'rider'], descending=[True, False])

    rows = insp.to_dicts()
    data_json = json.dumps(rows, ensure_ascii=False, default=str)

    html = """<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Inspección — Closer Riders</title>
<style>
  body { font-family: -apple-system, system-ui, sans-serif; margin: 0; padding: 24px;
         background: #f7f8f7; color: #1a1a1a; }
  h1 { font-size: 22px; margin: 0 0 4px; }
  .sub { color: #6b7280; font-size: 13px; margin-bottom: 16px; }
  .controls { margin-bottom: 16px; display: flex; gap: 8px; flex-wrap: wrap; }
  input, select { padding: 8px 12px; border: 1px solid #d1d5db; border-radius: 8px; font-size: 14px; }
  input { flex: 1; min-width: 200px; }
  table { width: 100%; border-collapse: collapse; background: white; border-radius: 12px;
          overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.08); }
  th, td { padding: 10px 12px; text-align: left; font-size: 13px; border-bottom: 1px solid #f0f0f0; }
  th { background: #f9fafb; font-weight: 600; text-transform: uppercase; font-size: 11px;
       letter-spacing: .04em; color: #6b7280; cursor: pointer; user-select: none; }
  th:hover { background: #f0f1f0; }
  tr:hover { background: #fafbfa; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .adj { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #10b981; }
  .stat { display: inline-block; margin-right: 20px; }
  .stat b { font-size: 20px; }
</style></head><body>
<h1>Inspección de datos — Closer Riders</h1>
<div class="sub">Verde = fila ajustada con Connections (regla 02:00). Click en cabeceras para ordenar.</div>
<div class="controls">
  <input id="q" placeholder="Buscar rider, móvil o ciudad...">
  <select id="dia"><option value="">Todos los días</option></select>
  <select id="adj">
    <option value="">Todas</option>
    <option value="1">Solo ajustadas</option>
    <option value="0">Solo sin ajustar</option>
  </select>
</div>
<div class="sub">
  <span class="stat"><b id="cRows">0</b> filas</span>
  <span class="stat"><b id="cRiders">0</b> riders</span>
  <span class="stat"><b id="cAdj">0</b> ajustadas</span>
</div>
<table id="t"><thead><tr>
  <th data-k="dia">Día</th><th data-k="rider">Rider</th><th data-k="movil">Móvil</th>
  <th data-k="ciudad">Ciudad</th><th data-k="viajes" class="num">Viajes</th>
  <th data-k="horas" class="num">Horas</th><th data-k="tph" class="num">TPH</th>
  <th data-k="aceptados" class="num">Acept.</th><th data-k="pct_acept" class="num">% Acept</th>
  <th data-k="pct_canc" class="num">% Canc</th><th data-k="ajustado">Aj.</th>
</tr></thead><tbody id="tb"></tbody></table>
<script>
const DATA = __DATA__;
let sortK = 'dia', sortDir = -1;
const dias = [...new Set(DATA.map(r => r.dia))].sort().reverse();
const selDia = document.getElementById('dia');
dias.forEach(d => { const o = document.createElement('option'); o.value = d; o.textContent = d; selDia.appendChild(o); });

function render() {
  const q = document.getElementById('q').value.toLowerCase();
  const fd = document.getElementById('dia').value;
  const fa = document.getElementById('adj').value;
  let rows = DATA.filter(r => {
    if (fd && r.dia !== fd) return false;
    if (fa === '1' && !r.ajustado) return false;
    if (fa === '0' && r.ajustado) return false;
    if (q) {
      const hay = ((r.rider||'') + (r.movil||'') + (r.ciudad||'')).toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });
  rows.sort((a,b) => {
    let va = a[sortK], vb = b[sortK];
    if (typeof va === 'number' && typeof vb === 'number') return (va-vb)*sortDir;
    return String(va).localeCompare(String(vb))*sortDir;
  });
  const tb = document.getElementById('tb');
  tb.innerHTML = rows.map(r => `<tr>
    <td>${r.dia||''}</td><td>${r.rider||''}</td><td>${r.movil||''}</td><td>${r.ciudad||''}</td>
    <td class="num">${r.viajes ?? ''}</td><td class="num">${r.horas ?? ''}</td>
    <td class="num">${r.tph ?? ''}</td><td class="num">${r.aceptados ?? ''}</td>
    <td class="num">${r.pct_acept != null ? r.pct_acept+'%' : ''}</td>
    <td class="num">${r.pct_canc != null ? r.pct_canc+'%' : ''}</td>
    <td>${r.ajustado ? '<span class="adj"></span>' : ''}</td>
  </tr>`).join('');
  document.getElementById('cRows').textContent = rows.length;
  document.getElementById('cRiders').textContent = new Set(rows.map(r=>r.movil)).size;
  document.getElementById('cAdj').textContent = rows.filter(r=>r.ajustado).length;
}
document.querySelectorAll('th').forEach(th => th.onclick = () => {
  const k = th.dataset.k;
  if (sortK === k) sortDir *= -1; else { sortK = k; sortDir = 1; }
  render();
});
document.getElementById('q').oninput = render;
document.getElementById('dia').onchange = render;
document.getElementById('adj').onchange = render;
render();
</script></body></html>"""

    html = html.replace('__DATA__', data_json)
    with open(DASHBOARD_HTML, 'w', encoding='utf-8') as f:
        f.write(html)


if __name__ == '__main__':
    main()
