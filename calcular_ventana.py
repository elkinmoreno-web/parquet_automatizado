# -*- coding: utf-8 -*-
"""
calcular_ventana.py — Detecta cuánto hay que descargar automáticamente.

Lee los bronze parquet ya existentes (que registran el último archivo/dato
procesado) y calcula cuántos DÍAS hacia atrás hay que bajar de Drive para
ponerse al día, sin importar cuánto tiempo lleve sin ejecutarse.

Imprime un único número: los días de --max-age a usar (con 1 día de margen).
Si no hay bronce (primera vez), imprime un valor grande para bajar todo.

Uso en el workflow:
    VENTANA=$(python3 calcular_ventana.py)
    rclone copy ... --max-age "${VENTANA}d" ...
"""
import os
import sys
import glob
from datetime import datetime, timezone

OUTPUT_DIR = os.environ.get('OUTPUT_DIR', 'datos_salida')
BRONZE_SUFFIX = os.environ.get('BRONZE_SUFFIX', '')

# Si no hay bronce, bajar un rango amplio (primera vez / seed)
DIAS_PRIMERA_VEZ = 30
# Margen de seguridad: siempre bajar 1 día extra por si un archivo llegó tarde
MARGEN_DIAS = 1
# Tope de seguridad para no pedir descargas absurdas
TOPE_DIAS = 30


def ultima_fecha_bronce():
    """Devuelve la fecha (UTC) del dato más reciente en los bronze, o None."""
    import polars as pl
    fechas = []

    # bronze_rta: tiene 'timestamp' (hora del pedido) y 'rta_file' (nombre con fecha)
    rta_path = os.path.join(OUTPUT_DIR, f'bronze_rta{BRONZE_SUFFIX}.parquet')
    if os.path.exists(rta_path):
        try:
            df = pl.read_parquet(rta_path, columns=['timestamp'])
            ts = df.with_columns(
                pl.col('timestamp').str.to_datetime(strict=False).alias('ts')
            ).filter(pl.col('ts').is_not_null())
            if len(ts) > 0:
                fechas.append(ts['ts'].max())
        except Exception as e:
            print(f"# aviso: no pude leer bronze_rta: {e}", file=sys.stderr)

    # bronze_connections: 'start_time'
    conn_path = os.path.join(OUTPUT_DIR, f'bronze_connections{BRONZE_SUFFIX}.parquet')
    if os.path.exists(conn_path):
        try:
            df = pl.read_parquet(conn_path, columns=['start_time'])
            ts = df.with_columns(
                pl.col('start_time').str.to_datetime(strict=False).alias('ts')
            ).filter(pl.col('ts').is_not_null())
            if len(ts) > 0:
                fechas.append(ts['ts'].max())
        except Exception as e:
            print(f"# aviso: no pude leer bronze_connections: {e}", file=sys.stderr)

    if not fechas:
        return None
    # Normalizar a naive (quitar tz) para comparar
    fechas = [f.replace(tzinfo=None) if f.tzinfo else f for f in fechas]
    return max(fechas)


def main():
    ultima = ultima_fecha_bronce()
    if ultima is None:
        # Primera ejecución: no hay bronce → bajar rango amplio
        print(DIAS_PRIMERA_VEZ)
        return

    ahora = datetime.now(timezone.utc).replace(tzinfo=None)
    dias_transcurridos = (ahora - ultima).days + 1  # +1 para redondear hacia arriba
    ventana = dias_transcurridos + MARGEN_DIAS
    ventana = max(2, min(ventana, TOPE_DIAS))  # entre 2 y TOPE días
    print(ventana)


if __name__ == '__main__':
    main()
