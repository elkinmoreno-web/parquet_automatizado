# -*- coding: utf-8 -*-
"""
Sincronización del resultado final del pipeline a la nueva Supabase.
Se llama UNA vez, al final de main(), después de escribir el parquet.

No recalcula nada — solo renombra columnas y ajusta unidades para que
coincidan con lo que espera la tabla driver_daily_stats, y sube el
resultado con upsert (courier_uuid, day).
"""

import os
import polars as pl
from supabase import create_client

# Mapeo columna del pipeline -> columna de la tabla driver_daily_stats.
# Los nombres NO coinciden 1:1 (ver ARCHITECTURE del CRM) — se resuelve
# aquí, en un solo lugar, sin tocar el procesamiento de arriba.
RENOMBRAR = {
    'driver_uuid': 'courier_uuid',
    'driver_email': 'email',
    'city_name': 'city',
    'cancel_not_at_fault_trips': 'cancel_not_at_fault',
    'tph_adj': 'tph',
}

COLUMNAS_DESTINO = [
    'day', 'courier_uuid', 'driver_name', 'driver_number', 'email', 'city',
    'flow_type', 'num_of_trips', 'online_hours', 'active_hours',
    'accept_trips', 'reject_trips', 'cancel_trips', 'cancel_not_at_fault',
    'tph', 'pct_accept', 'pct_cancel',
]


def sync_to_supabase(final: pl.DataFrame, ventana_dias: int = 21) -> None:
    """
    Sube a Supabase solo la ventana reciente (por defecto 21 días, igual
    que REPROCESS_WEEKS*7) — no hace falta reenviar el histórico completo
    en cada corrida, el UPSERT por (courier_uuid, day) ya cubre que los
    días de la ventana se sobrescriban con el dato más reciente.
    """
    df = final.clone()

    # 'day': en la rama normal (con Connections/RTA), la columna correcta
    # es '_dia' — ya trae la fecha lógica con la corrección de las 02:00
    # aplicada. En la rama "sin ajustar" (sin Connections ni RTA), esa
    # columna no existe y se usa 'datestr' directo (sin corrección,
    # porque en esa rama tampoco se aplicó ninguna).
    if '_dia' in df.columns:
        df = df.rename({'_dia': 'day'})
    elif 'datestr' in df.columns:
        df = df.with_columns(pl.col('datestr').dt.date().alias('day'))

    # pct_aceptacion / pct_cancelacion NO existen si esa fila nunca pasó
    # por el ajuste de Connections (rama "sin ajustar" de main()) — se
    # crean en cero para no romper el resto del proceso, no se inventa
    # un valor distinto de cero.
    for col_faltante in ('pct_aceptacion', 'pct_cancelacion', 'tph_adj', 'flow_type'):
        if col_faltante not in df.columns:
            df = df.with_columns(pl.lit(0.0 if col_faltante != 'flow_type' else None).alias(col_faltante))

    df = df.rename({k: v for k, v in RENOMBRAR.items() if k in df.columns})

    # pct_accept / pct_cancel: el pipeline los calcula en escala 0-100
    # (multiplicados por 100 en apply_adjustment); la tabla los guarda en
    # escala 0-1, que es lo que ya espera la interfaz del CRM (que
    # multiplica por 100 ella misma al mostrar el "%"). Se divide aquí,
    # una sola vez.
    df = df.with_columns([
        (pl.col('pct_aceptacion') / 100.0).alias('pct_accept'),
        (pl.col('pct_cancelacion') / 100.0).alias('pct_cancel'),
    ])

    for col in COLUMNAS_DESTINO:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    df = df.select(COLUMNAS_DESTINO)

    if 'day' in df.columns:
        from datetime import date, timedelta
        limite = date.today() - timedelta(days=ventana_dias)
        df = df.filter(pl.col('day') >= limite)

    registros = df.with_columns(pl.col('day').cast(pl.Utf8)).to_dicts()
    if not registros:
        print('[sync_supabase] Nada que sincronizar en la ventana reciente.')
        return

    supabase = create_client(os.environ["SUPABASE_METRICS_URL"], os.environ["SUPABASE_METRICS_SERVICE_KEY"])
    TAMANO_LOTE = 500
    subidos = 0
    for i in range(0, len(registros), TAMANO_LOTE):
        lote = registros[i:i + TAMANO_LOTE]
        supabase.table('driver_daily_stats').upsert(lote, on_conflict='courier_uuid,day').execute()
        subidos += len(lote)
        print(f'[sync_supabase] {subidos}/{len(registros)} filas sincronizadas')

    print(f'[sync_supabase] ✓ {subidos} filas sincronizadas a Supabase (ventana: últimos {ventana_dias} días)')
