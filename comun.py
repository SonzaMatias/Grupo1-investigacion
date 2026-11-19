import os
import time
from contextlib import contextmanager

import psycopg2
import pika

COLA_M2 = "cola_calificaciones_m2"

# Los nombres "db" y "broker" son los de docker-compose.yml. Se pueden cambiar con variables de entorno.
DB_HOST = os.getenv("DB_HOST", "db")
BROKER_HOST = os.getenv("BROKER_HOST", "broker")


@contextmanager
def transaccion_bd():
    """Abre conexión a PostgreSQL, hace COMMIT si todo sale bien o ROLLBACK si falla."""
    conexion = psycopg2.connect(
        host=DB_HOST, port=5432, dbname="bd_pedidos",
        user="usuario_tp", password="clave_tp"
    )
    cursor = conexion.cursor()
    try:
        yield cursor
        conexion.commit()
    except Exception:
        conexion.rollback()
        raise
    finally:
        cursor.close()
        conexion.close()


def conectar_broker(timeout=3, heartbeat=60):
    """Devuelve una conexión activa a RabbitMQ y su canal con la cola declarada."""
    parametros = pika.ConnectionParameters(
        host=BROKER_HOST, port=5672,
        connection_attempts=1, socket_timeout=timeout, heartbeat=heartbeat
    )
    conexion = pika.BlockingConnection(parametros)
    canal = conexion.channel()
    canal.queue_declare(queue=COLA_M2, durable=True)
    return conexion, canal


def log_flujo(calificacion_id, paso, direccion, mensaje):
    """Imprime un registro estandarizado con Correlation ID para trazabilidad."""
    print(f"[CALIFICACIÓN #{calificacion_id} | {paso} - {direccion}] {mensaje}")


# =====================================================================
# MODO PASO A PASO (solo para la exposición)
# =====================================================================
# Los servicios corren en contenedores separados y no pueden leer una tecla de la terminal.
# Por eso la señal de "Avanzar" pasa por la base de datos, que todos comparten:
#   - demo_control.paso_a_paso: si está apagado, las pausas no hacen nada.
#   - demo_pausas: una fila por servicio que está esperando. El botón "Avanzar" del panel
#     (POST /demo/avanzar) marca avanzar = TRUE y el servicio sigue.

def limpiar_pausa(servicio):
    """Borra una pausa que quedó colgada (por ejemplo, si el worker murió mientras esperaba)."""
    with transaccion_bd() as cursor:
        cursor.execute("DELETE FROM demo_pausas WHERE servicio = %s;", (servicio,))


def pausa(servicio, numero, descripcion, calificacion_id, esperar=time.sleep):
    """Si el modo paso a paso está activo, espera hasta que se presione "Avanzar" en el panel.

    esperar: función para esperar medio segundo. El relay y el worker pasan conexion.sleep de pika,
    que espera sin cortar la conexión con RabbitMQ (sigue respondiendo los heartbeats).
    """
    with transaccion_bd() as cursor:
        cursor.execute("SELECT paso_a_paso FROM demo_control WHERE id = 1;")
        if not cursor.fetchone()[0]:
            return
        cursor.execute(
            """
            INSERT INTO demo_pausas (servicio, numero, descripcion, calificacion_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (servicio) DO UPDATE
               SET numero = EXCLUDED.numero, descripcion = EXCLUDED.descripcion,
                   calificacion_id = EXCLUDED.calificacion_id, desde = now(), avanzar = FALSE;
            """,
            (servicio, numero, descripcion, calificacion_id),
        )
    log_flujo(calificacion_id, f"PAUSA {numero}", servicio.upper(), f"{descripcion} → esperando 'Avanzar' en el panel...")

    while True:
        esperar(0.5)
        with transaccion_bd() as cursor:
            # Sigue si se presionó "Avanzar" o si se apagó el modo paso a paso
            cursor.execute(
                """
                DELETE FROM demo_pausas
                 WHERE servicio = %s
                   AND (avanzar OR NOT (SELECT paso_a_paso FROM demo_control WHERE id = 1))
                RETURNING servicio;
                """,
                (servicio,),
            )
            if cursor.fetchone():
                return
            # Si la fila ya no existe (se presionó "Empezar de cero"), también sigue
            cursor.execute("SELECT 1 FROM demo_pausas WHERE servicio = %s;", (servicio,))
            if not cursor.fetchone():
                return


def generacion_actual(cursor):
    """Número de la demo actual: cambia cada vez que se presiona "Empezar de cero"."""
    cursor.execute("SELECT generacion FROM demo_control WHERE id = 1;")
    return cursor.fetchone()[0]
