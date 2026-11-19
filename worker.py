# Worker de M3 (Conductores): recibe las calificaciones de la cola y actualiza la reputación
# del conductor. Solo usa tablas de M3; nunca toca las tablas de M2.
import json
import os
import time

import pika
import psycopg2

from comun import COLA_M2, transaccion_bd, conectar_broker, log_flujo, pausa, limpiar_pausa, generacion_actual

TIEMPO_PROCESAMIENTO = float(os.getenv("TIEMPO_PROCESAMIENTO", "5"))  # segundos en estado Unacked


def sumar_a_la_reputacion(evento_id, conductor_id, puntaje, generacion):
    """Suma la calificación UNA sola vez. Devuelve "sumado", "duplicado" o "viejo".

    Las dos operaciones van en la misma transacción: o se registra el evento y se suma,
    o no pasa ninguna de las dos.
    """
    with transaccion_bd() as cursor:
        # Solo para la demo: un mensaje de antes de "Empezar de cero" se descarta
        if generacion is not None and generacion != generacion_actual(cursor):
            return "viejo"

        # 3A. Control de idempotencia: la clave primaria de eventos_procesados no admite repetidos.
        # Si el evento ya estaba, ON CONFLICT DO NOTHING no inserta nada (rowcount = 0).
        # Como es una sola operación, aunque dos workers lo intenten a la vez solo uno puede insertarlo.
        cursor.execute(
            "INSERT INTO eventos_procesados (evento_id) VALUES (%s) ON CONFLICT DO NOTHING;",
            (evento_id,),
        )
        if cursor.rowcount == 0:
            return "duplicado"

        # 4A. Sumar al promedio del conductor (crea la fila si es su primera calificación)
        cursor.execute(
            """
            INSERT INTO reputacion_conductor (conductor_id, cantidad, suma) VALUES (%s, 1, %s)
            ON CONFLICT (conductor_id) DO UPDATE
               SET cantidad = reputacion_conductor.cantidad + 1,
                   suma = reputacion_conductor.suma + EXCLUDED.suma;
            """,
            (conductor_id, puntaje),
        )
        return "sumado"


def procesar_mensaje(canal, method, properties, body):
    time.sleep(0.2)  # Micropausa para mantener el orden visual de los logs en Docker
    try:
        evento = json.loads(body.decode("utf-8"))
        evento_id = str(evento["evento_id"])
        calificacion_id = evento["calificacion_id"]
        conductor_id = evento["conductor_id"]
        puntaje = int(evento["puntaje"])
        generacion = evento.get("generacion")
    except (ValueError, KeyError, TypeError):
        # Un mensaje con formato inválido nunca se va a poder procesar: se descarta sin reencolar
        # para que no vuelva una y otra vez (y para que no tire abajo el worker).
        print(f"[WORKER - ERROR] Mensaje con formato inválido, se descarta: {body[:200]!r}")
        canal.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return

    reentrega = " [REENTREGADO tras caída previa sin ACK]" if method.redelivered else ""
    log_flujo(calificacion_id, "PASO 3", "WORKER <- BROKER", f"Recibido evento {evento_id} de '{conductor_id}' ({puntaje} estrellas){reentrega}.")
    pausa("worker", 3, f"El worker recibió el evento {evento_id} (Unacked) y todavía no lo procesó",
          calificacion_id, esperar=canal.connection.sleep)

    # 3B. Simulación del procesamiento asíncrono (el mensaje sigue en estado Unacked)
    log_flujo(calificacion_id, "PASO 3", "WORKER PROCESANDO", f"Recalculando reputación en M3 (demora {TIEMPO_PROCESAMIENTO:g}s; estado Unacked)...")
    canal.connection.sleep(TIEMPO_PROCESAMIENTO)

    try:
        resultado = sumar_a_la_reputacion(evento_id, conductor_id, puntaje, generacion)
    except psycopg2.Error as error:
        log_flujo(calificacion_id, "ERROR", "WORKER", f"Fallo en BD ({error}). Se envía NACK para reencolar mensaje.")
        canal.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    if resultado == "viejo":
        log_flujo(calificacion_id, "PASO 4", "WORKER", f"Evento {evento_id} de antes de 'Empezar de cero': se descarta.")
        canal.basic_ack(delivery_tag=method.delivery_tag)
        return
    if resultado == "sumado":
        log_flujo(calificacion_id, "PASO 4", "WORKER -> BD", f"COMMIT en BD de M3: evento {evento_id} registrado y {puntaje} sumado a la reputación de '{conductor_id}'.")
    else:
        log_flujo(calificacion_id, "PASO 4", "WORKER IDEMPOTENCIA", f"El evento {evento_id} ya estaba sumado. Se descarta el duplicado (no se suma de nuevo).")

    pausa("worker", 4, f"El worker ya guardó el evento {evento_id} (COMMIT) pero todavía no mandó el ACK "
                       "(si se cae ahora, RabbitMQ lo reentrega y la idempotencia lo frena)",
          calificacion_id, esperar=canal.connection.sleep)

    # 4B. Enviar ACK manual a RabbitMQ para retirar el mensaje de la cola
    canal.basic_ack(delivery_tag=method.delivery_tag)
    log_flujo(calificacion_id, "PASO 4", "WORKER -> BROKER", "Señal ACK enviada a RabbitMQ. Ciclo completado.\n")


def iniciar_worker():
    while True:
        conexion_broker = None
        try:
            limpiar_pausa("worker")  # por si el worker anterior murió mientras esperaba
            conexion_broker, canal = conectar_broker()
            canal.basic_qos(prefetch_count=1)
            canal.basic_consume(queue=COLA_M2, on_message_callback=procesar_mensaje, auto_ack=False)
            print(f"[INICIO WORKER] Escuchando '{COLA_M2}' (prefetch_count=1, auto_ack=False)...")
            canal.start_consuming()
        except (pika.exceptions.AMQPError, OSError) as error:
            print(f"[WORKER - ALERTA] Sin conexión con RabbitMQ ({type(error).__name__}). Reintentando en 3s...")
            time.sleep(3)
        except psycopg2.Error as error:
            print(f"[WORKER - ALERTA] Sin conexión con PostgreSQL ({type(error).__name__}). Reintentando en 3s...")
            time.sleep(3)
        finally:
            # Si se cortó a mitad de un mensaje, cerrar la conexión lo devuelve a la cola (Ready)
            if conexion_broker is not None and conexion_broker.is_open:
                try:
                    conexion_broker.close()
                except Exception:
                    pass


if __name__ == "__main__":
    iniciar_worker()
