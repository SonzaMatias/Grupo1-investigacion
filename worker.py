import json
import time
import pika
from comun import COLA_M2, transaccion_bd, conectar_broker, log_flujo

def procesar_mensaje(canal, method, properties, body):
    time.sleep(0.2)  # Micropausa para mantener el orden visual de los logs en Docker
    evento = json.loads(body.decode("utf-8"))
    calificacion_id = evento["calificacion_id"]

    reentrega = " [REENTREGADO tras caída previa sin ACK]" if method.redelivered else ""
    log_flujo(calificacion_id, "PASO 3", "WORKER <- BROKER", f"Recibido evento de '{evento['conductor_id']}' ({evento['puntaje']} estrellas){reentrega}.")

    try:
        # 3A. Control de Idempotencia (RF-2.5 y RNF-09): verificar si ya fue procesada
        with transaccion_bd() as cursor:
            cursor.execute("SELECT estado FROM calificaciones WHERE id = %s;", (calificacion_id,))
            fila = cursor.fetchone()

        if fila and fila[0] == "IMPACTADA_EN_CONDUCTOR":
            log_flujo(calificacion_id, "PASO 3", "WORKER IDEMPOTENCIA", "Calificación ya impactada anteriormente. Se descarta duplicado y se envía ACK.")
            canal.basic_ack(delivery_tag=method.delivery_tag)
            return

        # 3B. Simulación del procesamiento pesado asíncrono (40 segundos en estado Unacked)
        log_flujo(calificacion_id, "PASO 3", "WORKER PROCESANDO", "Recalculando reputación en M3 y auditando en M8 (demora 40s; estado Unacked)...")
        time.sleep(40)

        # 4A. Actualizar el estado final en la base de datos
        with transaccion_bd() as cursor:
            cursor.execute("UPDATE calificaciones SET estado = 'IMPACTADA_EN_CONDUCTOR' WHERE id = %s;", (calificacion_id,))
        log_flujo(calificacion_id, "PASO 4", "WORKER -> BD", "COMMIT en BD: estado actualizado a IMPACTADA_EN_CONDUCTOR.")

        # 4B. Enviar ACK manual a RabbitMQ para retirar el mensaje de la cola
        canal.basic_ack(delivery_tag=method.delivery_tag)
        log_flujo(calificacion_id, "PASO 4", "WORKER -> BROKER", "Señal ACK enviada a RabbitMQ. Ciclo completado.\n")

    except Exception as error:
        log_flujo(calificacion_id, "ERROR", "WORKER", f"Fallo en BD ({error}). Se envía NACK para reencolar mensaje.")
        canal.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def iniciar_worker():
    while True:
        try:
            conexion_broker, canal = conectar_broker(heartbeat=300)
            canal.basic_qos(prefetch_count=1)
            canal.basic_consume(queue=COLA_M2, on_message_callback=procesar_mensaje, auto_ack=False)
            print(f"[INICIO WORKER] Escuchando '{COLA_M2}' (prefetch_count=1, auto_ack=False)...")
            canal.start_consuming()
        except (pika.exceptions.AMQPError, OSError) as error:
            print(f"[WORKER - ALERTA] Sin conexión con RabbitMQ ({type(error).__name__}). Reintentando en 10s...")
            time.sleep(10)

if __name__ == "__main__":
    iniciar_worker()