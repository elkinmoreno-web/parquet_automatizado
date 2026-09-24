# -*- coding: utf-8 -*-
"""
Pipeline Closer Logistics — Viajes + Connections con regla de las 02:00
=======================================================================
VERSIÓN: v5.5-cierre-dia  (2026-09-24)

  TODO se calcula desde Connections (viajes, horas, aceptados, cancelados,
  % aceptación, % cancelación). El silver solo aporta metadatos del rider.
  Días sin cobertura de Connections se descartan.

CAMBIOS DE ESTA VERSIÓN (v5.5) — ver comentarios marcados [FIX 6] y [FIX 7]:

  [FIX 6] El pipeline marca cada día como CERRADO al terminar de subirlo.
  [FIX 7] Un aborto por falta de datos sale en ROJO en GitHub Actions.

CAMBIOS DE LA VERSIÓN ANTERIOR (v5.4) — comentarios marcados [FIX 1..5]:

  Síntoma: el TPH salía a la mitad de lo que muestran Uber y el dashboard
  de BigQuery. Un rider con 33,73 h y 65 viajes (TPH 1,93) aparecía aquí
  con 33,73 h y 42 viajes (TPH 1,25). Medido sobre 16.921 filas: el 31%
  tenía las horas infladas respecto a sus propios viajes.

  Prueba que lo identificó — viajes por hora, agrupando por si la fila era
  "rara" (active_hours < 60% de online_hours):

                          filas normales   filas raras (31%)
      viajes / hora ACTIVA     2,72             2,82   <- iguales
      viajes / hora ONLINE     2,21             1,21   <- la mitad

  num_of_trips y active_hours son coherentes entre sí SIEMPRE. La columna
  que se desmadraba era online_hours, porque se sustituía sola.
"""

PIPELINE_VERSION = "v5.5-cierre-dia"

import os
import sys
import io
import re
import glob
import json
import gzip
import base64
from datetime import datetime, date, timedelta, timezone
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

# Cuántos días de histórico se conservan en los bronze.
# Antes NO se borraba nada: el parquet crecía indefinidamente, y con él la
# memoria del runner (de ahí los SIGTERM / exit 143), el tiempo de dedup y
# sobre todo el de subida a Drive (llegó a 3m54s, el 41% del run).
# Es seguro podar porque el silver solo usa REPROCESS_WEEKS (3 semanas) y las
# descargas traen --max-age 3d.
# NO bajar de ~15 días: el registro de ficheros ya procesados (dedup) se
# deriva de las filas guardadas, y debe cubrir de sobra la ventana de descarga.
BRONZE_RETENTION_DAYS = int(os.environ.get('BRONZE_RETENTION_DAYS', '60'))

# Hora de corte del día lógico (registros antes de esto → día anterior)
LOGICAL_DAY_CUTOFF_HOUR = 2

# UUID de un rider a inspeccionar (opcional). Si se define, el pipeline imprime
# sus filas crudas del bronze antes de consolidar. Sirve para contrastar un
# caso concreto contra el panel de Uber sin tener que adivinar:
#   DEBUG_UUID=2c7de639-9911-4736-8b50-cbd21e6e1dee python proceso_completo_uber.py
DEBUG_UUID = os.environ.get('DEBUG_UUID', '').strip()

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


def podar_bronze(df, columna_fecha, etiqueta):
    """
    Recorta el bronze a los últimos BRONZE_RETENTION_DAYS.

    Sin esto el parquet crece para siempre: cada ejecución lo lee entero, le
    añade lo nuevo y lo reescribe completo, así que el coste de memoria, de
    dedup y de subida a Drive sube semana a semana sin tope.
    """
    if df is None or len(df) == 0 or columna_fecha not in df.columns:
        return df

    limite = datetime.now() - timedelta(days=BRONZE_RETENTION_DAYS)
    ts = pl.col(columna_fecha)
    if df.schema[columna_fecha] == pl.Utf8:
        ts = ts.str.to_datetime(strict=False)

    antes = len(df)
    # Las filas con fecha ilegible se CONSERVAN: preferimos guardar de más
    # antes que tirar datos por un formato inesperado.
    df = df.filter(ts.is_null() | (ts >= limite))
    if antes != len(df):
        print(f"[{etiqueta}] Poda a {BRONZE_RETENTION_DAYS} días: {antes:,} → {len(df):,} filas")
    return df


def avisar_errores(etiqueta, errores, total):
    """
    Un CSV corrupto no debe pasar desapercibido: antes solo se imprimía el
    error y el run terminaba en verde con datos incompletos, que es el peor
    de los fallos posibles (nadie se entera hasta que faltan riders).
    """
    if not errores:
        return
    print(f"[{etiqueta}] ⚠ {errores}/{total} ficheros fallaron al parsear")
    if total and errores > total / 2:
        raise RuntimeError(
            f"[{etiqueta}] Demasiados ficheros corruptos ({errores}/{total}): "
            "se aborta para no publicar datos incompletos"
        )


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

    # ------------------------------------------------------------------ [FIX 1]
    # Se procesan TODAS las exportaciones, no solo la última de cada día.
    #
    # Uber exporta varias veces al día (06:56, 08:54, 11:50, 12:51, 17:19...).
    # CADA exportación lleva el día en curso CORTADO a esa hora: la de las
    # 17:19 no tiene las tardes ni las noches. El día solo queda completo en
    # una exportación POSTERIOR, normalmente la del día siguiente.
    #
    # El código anterior se quedaba con una sola exportación por día de
    # exportación, así que muchos días se quedaban congelados en su versión
    # parcial y nunca llegaba la buena. De ahí que los viajes salieran ~35%
    # por debajo de lo que muestra Uber.
    #
    # Procesarlas todas no duplica nada: build_silver ya ordena por file_date
    # descendente y se queda con la fila más reciente de cada
    # (driver_uuid, datestr, zona). Simplemente hoy no tiene entre qué elegir.
    #
    # Es además lo que hace el cargador de BigQuery (WRITE_APPEND de todos los
    # ficheros + una vista que elige una fila por conductor y día), y por eso
    # sus números sí cuadran con Uber.
    candidatos = sorted(files, key=lambda f: f['ts'])
    # -------------------------------------------------------------------------

    nuevos = [f for f in candidatos if f['name'] not in processed]
    print(f"[daily] Archivos nuevos a procesar: {len(nuevos)} (de {len(files)} en disco)")

    new_dfs = []
    errores = 0
    for i, f in enumerate(nuevos, 1):
        try:
            df = parse_daily_csv(f['path'], f['name'], f['ts'])
            new_dfs.append(df)
            print(f"  [{i}/{len(nuevos)}] {f['name']}: {len(df):,} filas")
        except Exception as e:
            errores += 1
            print(f"  [{i}/{len(nuevos)}] {f['name']}: ERROR {e}")
    avisar_errores('daily', errores, len(nuevos))

    # Si no hay nada nuevo Y no hay nada que podar, NO se reescribe el parquet:
    # así conserva su fecha de modificación y rclone se salta la subida a Drive.
    if not new_dfs and existing is not None:
        podado = podar_bronze(existing, 'datestr', 'daily')
        if len(podado) == len(existing):
            print("[daily] Sin archivos nuevos ni poda pendiente — no se reescribe el parquet")
            return existing
        podado.write_parquet(BRONZE_DAILY_PARQUET, compression='zstd')
        print(f"[daily] Bronze guardado (solo poda): {len(podado):,} filas")
        return podado

    parts = []
    if existing is not None: parts.append(existing)
    if new_dfs:              parts.append(pl.concat(new_dfs, how='vertical_relaxed'))
    if not parts:
        return None
    bronze = pl.concat(parts, how='vertical_relaxed') if len(parts) > 1 else parts[0]
    bronze = podar_bronze(bronze, 'datestr', 'daily')
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
        # [FIX 4] Dedup DENTRO del bucle, no solo al final.
        # Los ficheros de CONNECTIONS se solapan muchísimo: 234 ficheros de
        # ~300.000 filas son ~70 millones de filas de las que solo ~3,3 quedan
        # tras dedupar. Acumulando todo y dedupando al final, el pico de
        # memoria es el de las 70 millones — el runner (7 GB) muere con
        # SIGTERM / exit 143. Dedupando en cada lote, 'acumulado' nunca pasa
        # del tamaño ya deduplicado. El resultado es idéntico.
        acumulado = acumulado.unique(subset=['courier_uuid', 'start_time', 'status'], keep='first')
        lote_actual = []

    errores = 0
    for i, f in enumerate(nuevos, 1):
        try:
            df = parse_conn_csv(f['path'], f['name'])
            lote_actual.append(df)
            print(f"  [{i}/{len(nuevos)}] {f['name']}: {len(df):,} filas")
        except Exception as e:
            errores += 1
            print(f"  [{i}/{len(nuevos)}] {f['name']}: ERROR {e}")
        if len(lote_actual) >= LOTE_TAMANO:
            volcar_lote()
    volcar_lote()
    avisar_errores('conn', errores, len(nuevos))

    if acumulado is None:
        return None
    bronze = acumulado

    # Dedup incremental: (courier_uuid, start_time, status) — los archivos pueden solapar
    before = len(bronze)
    bronze = bronze.unique(subset=['courier_uuid', 'start_time', 'status'], keep='first')
    print(f"[conn] Dedup: {before:,} → {len(bronze):,} filas")

    bronze = podar_bronze(bronze, 'start_time', 'conn')
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
        # [FIX 4] Mismo motivo que en connections: dedupar en cada lote para
        # que el acumulado no crezca con todas las repeticiones a la vez.
        acumulado_rta = acumulado_rta.unique(subset=['offer_id'], keep='first')
        lote_actual_rta = []

    errores = 0
    for i, f in enumerate(nuevos, 1):
        try:
            df = parse_rta_csv(f['path'], f['name'])
            lote_actual_rta.append(df)
        except Exception as e:
            errores += 1
            print(f"  [{i}/{len(nuevos)}] {f['name']}: ERROR {e}")
        if len(lote_actual_rta) >= LOTE_TAMANO:
            volcar_lote_rta()
    volcar_lote_rta()
    avisar_errores('rta', errores, len(nuevos))

    if acumulado_rta is None:
        return None
    bronze = acumulado_rta

    # Dedup incremental por offer_id (cada pedido es único; archivos solapan)
    before = len(bronze)
    bronze = bronze.unique(subset=['offer_id'], keep='first')
    print(f"[rta] Dedup por offer_id: {before:,} → {len(bronze):,} filas")

    bronze = podar_bronze(bronze, 'timestamp', 'rta')
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

    # --- Diagnóstico: cuántos (rider, día) tienen VARIOS mercados ---
    # Es lo que [FIX 5] recupera. Si sale 0, este pipeline no tiene el problema
    # de los mercados perdidos y hay que buscar la diferencia en otro sitio.
    if 'market_name' in base.columns:
        _multi = (
            base.group_by(['driver_uuid', 'datestr'])
                .agg(pl.col('market_name').n_unique().alias('_n'))
                .filter(pl.col('_n') > 1)
        )
        print(f"[silver] (rider, día) con más de un mercado: {len(_multi):,}")

    # --- Diagnóstico de un rider concreto (DEBUG_UUID) ---
    if DEBUG_UUID:
        _d = base.filter(pl.col('driver_uuid') == DEBUG_UUID)
        print(f"\n[debug] Filas crudas del bronze para {DEBUG_UUID}: {len(_d)}")
        _cols = [c for c in ['datestr', 'market_name', 'city_name', 'file_name',
                             'online_hours', 'active_hours', 'num_of_trips'] if c in _d.columns]
        for fila in _d.sort('datestr').select(_cols).iter_rows(named=True):
            print('[debug]  ' + '  '.join(
                f"{k}={round(v, 2) if isinstance(v, float) else v}" for k, v in fila.items()))
        print()

    # ------------------------------------------------------------------ [FIX 5]
    # --- Paso 1: quedarse con la exportación MÁS RECIENTE de cada (uuid, día),
    #     conservando TODAS sus filas de mercado ---
    #
    # Uber exporta UNA FILA POR MERCADO. Un rider que trabaja en dos mercados
    # de la misma ciudad (p. ej. MADRID CARABANCHEL y MADRID CENTRO) sale en
    # dos filas con el MISMO city_id y city_name, y solo se distinguen por
    # market_name.
    #
    # La clave de dedup anterior era (driver_uuid, datestr, city_id,
    # city_name) — sin market_name. Las dos filas colisionaban y unique()
    # descartaba una: el día de ese rider perdía un mercado entero.
    # Medido sobre un CSV real: en las combinaciones (rider, día) con dos
    # mercados, quedarse con una sola fila conserva el 67% de las horas.
    #
    # No basta con añadir market_name a la clave. Si una exportación vieja
    # tenía al rider en el mercado A y una nueva lo reclasifica al B, sumar
    # ambas contaría de más. Por eso se hace en dos tiempos:
    #   1. Por cada (rider, día), la exportación más reciente que lo contenga.
    #   2. De ESA exportación, todas sus filas de mercado.
    # Así nunca se mezclan versiones distintas del mismo día.
    ultimo_por_dia = (
        base.group_by(['driver_uuid', 'datestr'])
            .agg(pl.col('file_date').max().alias('_file_mas_reciente'))
    )
    base = (
        base.join(ultimo_por_dia, on=['driver_uuid', 'datestr'], how='inner')
            .filter(pl.col('file_date') == pl.col('_file_mas_reciente'))
            .drop('_file_mas_reciente')
    )

    # Dentro de esa exportación, una fila por mercado (por si el propio
    # fichero trae la misma repetida).
    zona_cols = [c for c in ['city_id', 'city_name', 'market_name'] if c in base.columns]
    ident_keys = ['driver_uuid', 'datestr'] + zona_cols
    base = (
        base.sort(['num_of_trips'], descending=[True], nulls_last=True)
            .unique(subset=ident_keys, keep='first', maintain_order=True)
    )
    # -------------------------------------------------------------------------

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

    OJO: la ÚLTIMA sesión de cada rider no tiene evento siguiente, así que su
    end_eff queda nulo y se descarta. Por eso el día en curso siempre entra
    algo corto y se completa en la exportación siguiente — tenlo en cuenta si
    algún día bajas el colchón de 2 días del dashboard.

    horas_conn ya NO se usa para sustituir online_hours (ver [FIX 2]): solo
    queda como diagnóstico, para poder medir cuántos días del silver siguen
    llegando incompletos.

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

    La fracción de madrugada se calcula con timestamps reales de Connections.

    Para cada (rider, día D):
      mover = total_silver(D) * frac_horas(D)   → de D a D-1
    Si un día no tiene Connections, su fracción es 0 → no se mueve nada.

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

    # ------------------------------------------------------------------ [FIX 2]
    # ELIMINADA la sustitución de online_hours por horas_conn.
    #
    # Lo que hacía: si Connections y el CSV diferían en más de 1 hora, se
    # pisaba online_hours con el valor de Connections. El problema es que
    # cuando la fila del CSV viene incompleta, NO SOLO las horas están
    # incompletas: también num_of_trips y active_hours. Al corregir una sola
    # columna, la fila queda incoherente y tph = viajes/horas sale a la mitad.
    #
    # Una fila parcial, entera, es coherente (pocos viajes y pocas horas → TPH
    # correcto). Parcheando solo las horas se rompe eso.
    #
    # Medido sobre 16.921 filas de producción antes de este cambio:
    #                           filas normales   filas parcheadas (31%)
    #     viajes / hora ACTIVA      2,72               2,82   <- iguales
    #     viajes / hora ONLINE      2,21               1,21   <- la mitad
    #
    # Con [FIX 1] las filas del CSV ya llegan completas, así que esta muleta
    # no hace falta. Se conserva solo como DIAGNÓSTICO: si el aviso de abajo
    # sigue apareciendo mucho, es que el CSV sigue llegando incompleto y hay
    # que mirar la descarga, no parchear columnas sueltas.
    if 'horas_conn' in s.columns and 'online_hours' in s.columns:
        _desajustadas = s.filter(
            pl.col('horas_conn').is_not_null() &
            ((pl.col('horas_conn') - pl.col('online_hours')).abs() > 1.0)
        ).height
        if _desajustadas:
            _pct = 100.0 * _desajustadas / max(len(s), 1)
            print(f"[ajuste] Días donde Connections y el CSV difieren >1h: "
                  f"{_desajustadas:,} ({_pct:.1f}%) — NO se sustituye nada")
            if _pct > 10:
                print("[ajuste] ⚠ Por encima del 10%: el CSV de COURIER_DAILY "
                      "está llegando incompleto, revisar la descarga")
    # -------------------------------------------------------------------------

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

    # ------------------------------------------------------------------ [FIX 3]
    # Solo se mueve a un día que EXISTA como fila.
    #
    # El traspaso se hacía restando de D y sumando a D-1 con un left join. Si
    # D-1 no existía (el rider no trabajó ese día, o D es el día más antiguo de
    # la ventana), el join no encontraba destino y lo restado NO SE SUMABA EN
    # NINGÚN SITIO: esos viajes y esas horas desaparecían. En el día más
    # antiguo de la ventana pasaba SIEMPRE.
    #
    # Se marca cada fila con si su D-1 existe, y si no existe se pone su
    # fracción a 0: el día conserva sus totales íntegros, que es preferible a
    # perderlos.
    _destinos = (
        s.select(['driver_uuid', '_dia']).unique()
         .with_columns((pl.col('_dia') + pl.duration(days=1)).alias('_dia'))
         .with_columns(pl.lit(True).alias('_destino_existe'))
    )
    s = s.join(_destinos, on=['driver_uuid', '_dia'], how='left')
    s = s.with_columns(pl.col('_destino_existe').fill_null(False))

    _sin_destino = s.filter((pl.col('frac_horas') > 0) & ~pl.col('_destino_existe')).height
    if _sin_destino:
        print(f"[ajuste] {_sin_destino:,} días con madrugada pero sin día anterior "
              f"en la ventana: se dejan íntegros (antes se perdían)")

    s = s.with_columns(
        pl.when(pl.col('_destino_existe')).then(pl.col('frac_horas'))
          .otherwise(0.0).alias('frac_horas')
    )
    # -------------------------------------------------------------------------

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
    aux = ['frac_rta', 'frac_horas', 'rta_madrugada', 'horas_conn', '_destino_existe'] + \
          ['mv_' + c for c in PEDIDO_COLS + HORA_COLS] + \
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


def sync_to_supabase(final: pl.DataFrame, ventana_dias: int = None) -> None:
    """
    Sube a Supabase solo la ventana reciente. El UPSERT por (courier_uuid, day)
    cubre que los días de la ventana se sobrescriban con el dato más reciente.

    La ventana por defecto son 7 días, no 21: las descargas traen --max-age 3d,
    así que más allá de ~4 días NADA puede haber cambiado. Subir 21 días en
    cada una de las 4 ejecuciones diarias eran ~59.000 filas (~118 peticiones)
    para modificar unas pocas miles — y además el runner está en EE. UU. y la
    base en Irlanda, con lo que cada petición cruza el Atlántico.

    Para un rebackfill completo: SUPABASE_SYNC_DAYS=21 (o más) al lanzarlo.
    """
    if ventana_dias is None:
        ventana_dias = int(os.environ.get('SUPABASE_SYNC_DAYS', '7'))

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
    # 1000 filas por petición (~250 KB): la mitad de viajes de ida y vuelta a
    # Irlanda que con lotes de 500, muy por debajo del límite de tamaño.
    TAMANO_LOTE = 1000
    subidos = 0
    for i in range(0, len(registros), TAMANO_LOTE):
        lote = registros[i:i + TAMANO_LOTE]
        supabase.table('driver_daily_stats').upsert(lote, on_conflict='courier_uuid,day').execute()
        subidos += len(lote)
        print(f'[sync_to_supabase] {subidos}/{len(registros)} filas sincronizadas')

    print(f'[sync_to_supabase] {subidos} filas sincronizadas a Supabase (ventana: ultimos {ventana_dias} dias)')

    # ------------------------------------------------------------------ [FIX 6]
    # Marca de "dia cerrado" en la tabla pipeline_cargas.
    #
    # Va DESPUES del bucle a proposito: solo se escribe si TODOS los lotes han
    # subido bien. Si el proceso muere a medias, la marca se queda con la fecha
    # anterior y el CRM sabe que el dia no esta listo.
    #
    # Lo necesita el aviso de TPH a los riders. Antes el CRM miraba el
    # created_at mas reciente de driver_daily_stats para decidir si podia
    # enviar, y como esta subida va por lotes de 1.000 con UPSERT, en cuanto
    # aterrizaba el PRIMER lote ya daba verde con miles de filas por subir.
    # Resultado medido: el 22-sep salieron 42 correos de 306 riders en rojo, y
    # el 23-sep, 28 de 295. Con esta marca la decision pasa a ser exacta en
    # lugar de estimada.
    conteos = {}
    for r in registros:
        conteos[r['day']] = conteos.get(r['day'], 0) + 1

    ahora = datetime.now(timezone.utc).isoformat()
    supabase.table('pipeline_cargas').upsert(
        [{'dia': dia, 'filas': n, 'cerrado_en': ahora} for dia, n in conteos.items()],
        on_conflict='dia',
    ).execute()
    print(f'[sync_to_supabase] Dias marcados como cerrados: {len(conteos)} -> {sorted(conteos)}')
    # -------------------------------------------------------------------------


# =============================================================================
# 5. MAIN
# =============================================================================

def main():
    # --- Bronze ---
    bronze_daily = ingest_bronze_daily()
    if bronze_daily is None:
        print("\n✗ No hay datos de COURIER_DAILY. Abortando.")
        # ------------------------------------------------------------- [FIX 7]
        # sys.exit(1) y NO 'return': con return el script terminaba con codigo
        # 0 y GitHub Actions marcaba la ejecucion en VERDE aunque no hubiera
        # ingerido ni una fila. Un fallo de descarga (por ejemplo el token de
        # Drive caducado) se veia igual que una ejecucion correcta.
        sys.exit(1)
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

    # --- Control de calidad: utilización media por día de la semana ---
    # OJO con cómo se lee esto: el % de horas activas NO es un indicador de
    # dato roto. Tiene un patrón semanal real y fuerte (medido sobre 4
    # semanas): lunes-jueves ronda el 63-68% y viernes-domingo el 72-76%,
    # simplemente porque entre semana hay menos pedidos y el rider espera más.
    # Un jueves "bajo" es un jueves normal.
    # Lo que sí es sospechoso es que un día se salga del patrón de SU MISMO
    # día de la semana en otras semanas. Por eso se imprime desglosado, para
    # poder comparar manzanas con manzanas.
    _qc = final.filter((pl.col('online_hours') > 2) & (pl.col('active_hours') > 0.5))
    if len(_qc) > 100:
        _por_dia = (
            _qc.with_columns([
                pl.col('_dia').dt.weekday().alias('_dow'),
                (pl.col('active_hours') / pl.col('online_hours')).alias('_util'),
            ])
            .group_by('_dow').agg([pl.len().alias('filas'), pl.col('_util').mean().alias('util')])
            .sort('_dow')
        )
        nombres = {1: 'Lun', 2: 'Mar', 3: 'Mie', 4: 'Jue', 5: 'Vie', 6: 'Sab', 7: 'Dom'}
        print('[qc] Utilización media (activas/online) por día de la semana:')
        for fila in _por_dia.iter_rows(named=True):
            print(f"[qc]   {nombres.get(fila['_dow'], '?'):<4} {fila['util']*100:5.1f}%  ({fila['filas']:,} filas)")

    # --- Salida: SOLO el parquet (lo que lee la app). Sin CSV ni dashboard. ---
    final.write_parquet(SILVER_PARQUET, compression='zstd')
    print(f"\n✓ Parquet final: {SILVER_PARQUET} ({len(final):,} filas)")

    # --- Sincronizar el resultado a Supabase (reemplaza a la API Fleet Manager) ---
    sync_to_supabase(final)

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
