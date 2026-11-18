from contextlib import contextmanager
import psycopg2
import pika

COLA_M2 = "cola_calificaciones_m2"

@contextmanager
def transaccion_bd():
    """Abre conexión a PostgreSQL, hace COMMIT si todo sale bien o ROLLBACK si falla."""
    conexion = psycopg2.connect(
        host="db", port=5432, dbname="bd_pedidos",
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
        host="broker", port=5672,
        connection_attempts=1, socket_timeout=timeout, heartbeat=heartbeat
    )
    conexion = pika.BlockingConnection(parametros)
    canal = conexion.channel()
    canal.queue_declare(queue=COLA_M2, durable=True)
    return conexion, canal

def log_flujo(calificacion_id, paso, direccion, mensaje):
    """Imprime un registro estandarizado con Correlation ID para trazabilidad."""
    print(f"[CALIFICACIÓN #{calificacion_id} | {paso} - {direccion}] {mensaje}")