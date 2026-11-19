import json
import time
import pika
from comun import COLA_M2, COLA_DLQ, transaccion_bd, conectar_broker, log_flujo

def procesar_mensaje(canal, method, properties, body):
    time.sleep(0.2)  # Micropausa para mantener el orden visual de logs en Docker

    # 3A. Validación de formato y contrato (Defensa contra Poison Pills -> DLQ)
    try:
        evento = json.loads(body.decode("utf-8"))
        calificacion_id = int(evento["calif_id"])
        conductor_id = str(evento["conductor"])
        puntaje = int(evento["puntaje"])
        if not (1 <= puntaje <= 5):
            raise ValueError(f"Puntaje fuera de rango (1..5): {puntaje}")
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, ValueError, TypeError) as error_formato:
        print(f"[POISON PILL -> DLQ | WORKER M3] Mensaje corrupto descartado ({type(error_formato).__name__}: {error_formato}). Desviando a '{COLA_DLQ}'...")
        canal.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    reentrega = " [REENTREGADO tras caída previa sin ACK]" if method.redelivered else ""
    log_flujo(calificacion_id, "PASO 3", "WORKER M3 <- BROKER", f"Recibido evento para '{conductor_id}' ({puntaje} estrellas){reentrega}.")

    try:
        # 3B y 4A. Candado no bloqueante + Inbox Atómico + Reputación en UNA transacción
        estado_idempotencia = None
        with transaccion_bd() as cursor:
            # Filtro 1 (En vuelo): intenta tomar un candado exclusivo sin bloquearse si ya está ocupado
            cursor.execute("SELECT pg_try_advisory_xact_lock(%s);", (calificacion_id,))
            candado_obtenido = cursor.fetchone()[0]

            if not candado_obtenido:
                estado_idempotencia = "EN_CURSO"
            else:
                # Filtro 2 (Histórico): verifica e inserta en el Inbox permanente
                cursor.execute(
                    """
                    INSERT INTO m3.inbox_eventos (calif_id)
                    VALUES (%s)
                    ON CONFLICT (calif_id) DO NOTHING
                    RETURNING calif_id;
                    """,
                    (calificacion_id,)
                )
                if cursor.fetchone() is None:
                    estado_idempotencia = "YA_PROCESADO"
                else:
                    # Simulación de procesamiento pesado asíncrono (40s en estado Unacked)
                    log_flujo(calificacion_id, "PASO 3", "WORKER M3 PROCESANDO", f"Candado adquirido. Recalculando reputación de '{conductor_id}' (demora 40s; Unacked)...")
                    time.sleep(40)

                    # Upsert atómico de la reputación del conductor en el esquema M3
                    cursor.execute(
                        """
                        INSERT INTO m3.conductores_reputacion (conductor, cant, suma, promedio, actualizado)
                        VALUES (%s, 1, %s, %s, CURRENT_TIME)
                        ON CONFLICT (conductor) DO UPDATE SET
                            cant = m3.conductores_reputacion.cant + 1,
                            suma = m3.conductores_reputacion.suma + EXCLUDED.suma,
                            promedio = ROUND(
                                (m3.conductores_reputacion.suma + EXCLUDED.suma)::NUMERIC
                                / (m3.conductores_reputacion.cant + 1), 2
                            ),
                            actualizado = CURRENT_TIME
                        RETURNING cant, promedio;
                        """,
                        (conductor_id, puntaje, puntaje)
                    )
                    cantidad, promedio = cursor.fetchone()

        # Salidas por idempotencia ya fuera del bloque 'with' (conexión a BD liberada antes del ACK)
        if estado_idempotencia == "EN_CURSO":
            log_flujo(calificacion_id, "PASO 3", "WORKER M3 CONCURRENCIA", "Otro worker ya procesa este ID. Se descarta duplicado al instante (sin pausar) y se envía ACK.")
            canal.basic_ack(delivery_tag=method.delivery_tag)
            return

        if estado_idempotencia == "YA_PROCESADO":
            log_flujo(calificacion_id, "PASO 3", "WORKER M3 IDEMPOTENCIA", "Ya registrado en 'm3.inbox_eventos'. Se descarta duplicado y se envía ACK.")
            canal.basic_ack(delivery_tag=method.delivery_tag)
            return

        log_flujo(calificacion_id, "PASO 4", "WORKER M3 -> BD M3", f"COMMIT atómico (Inbox + Reputación): '{conductor_id}' tiene {cantidad} calif. (promedio {promedio}).")

        # 4B. Confirmación manual a RabbitMQ recién después del COMMIT en M3
        canal.basic_ack(delivery_tag=method.delivery_tag)
        log_flujo(calificacion_id, "PASO 4", "WORKER M3 -> BROKER", "Señal ACK enviada a RabbitMQ. Ciclo completado.\n")

    except Exception as error:
        log_flujo(calificacion_id, "ERROR", "WORKER M3", f"Fallo transitorio en BD M3 ({type(error).__name__}). Esperando 3s antes de NACK con requeue...")
        time.sleep(3)
        canal.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def iniciar_worker():
    while True:
        try:
            conexion_broker, canal = conectar_broker(heartbeat=300)
            canal.basic_qos(prefetch_count=1)
            canal.basic_consume(queue=COLA_M2, on_message_callback=procesar_mensaje, auto_ack=False)
            print(f"[INICIO WORKER M3] Escuchando '{COLA_M2}' (prefetch_count=1, auto_ack=False, DLQ='{COLA_DLQ}')...")
            canal.start_consuming()
        except (pika.exceptions.AMQPError, OSError) as error:
            print(f"[WORKER M3 - ALERTA] Sin conexión con RabbitMQ ({type(error).__name__}). Reintentando en 10s...")
            time.sleep(10)

if __name__ == "__main__":
    iniciar_worker()