import json
import time

import pika
import psycopg2

from comun import COLA_M2, transaccion_bd, conectar_broker, log_flujo, pausa, limpiar_pausa

ESPERA_SI_BROKER_CAIDO = 3  # segundos antes de reintentar


def retransmitir_eventos_outbox():
    # 2A. Consultar si existen eventos pendientes en la base de datos
    with transaccion_bd() as cursor:
        cursor.execute("SELECT id, calificacion_id, payload FROM outbox_eventos WHERE estado = 'PENDIENTE' ORDER BY id ASC;")
        eventos_pendientes = cursor.fetchall()

    if not eventos_pendientes:
        return

    # 2B. Si hay eventos, abrir conexión con RabbitMQ y publicar con confirmación
    try:
        conexion_broker, canal = conectar_broker()
        canal.confirm_delivery()

        for evento_id, calificacion_id, payload_json in eventos_pendientes:
            log_flujo(calificacion_id, "PASO 2", "RELAY <- BD", f"Evento #{evento_id} leído en estado PENDIENTE.")
            pausa("relay", 1, f"El evento #{evento_id} está PENDIENTE en el outbox y el relay todavía no lo publicó",
                  calificacion_id, esperar=conexion_broker.sleep)

            # El evento_id viaja en el mensaje: M3 lo usa para no sumar dos veces el mismo evento
            mensaje = json.dumps({"evento_id": f"outbox-{evento_id}", **json.loads(payload_json)})
            log_flujo(calificacion_id, "PASO 2", "RELAY -> BROKER", f"Publicando Evento #{evento_id} en '{COLA_M2}'...")
            canal.basic_publish(
                exchange="", routing_key=COLA_M2, body=mensaje,
                properties=pika.BasicProperties(delivery_mode=2, content_type="application/json")
            )
            pausa("relay", 2, f"El evento #{evento_id} ya está en RabbitMQ pero sigue PENDIENTE en el outbox "
                              "(si el relay se cae ahora, lo vuelve a publicar)",
                  calificacion_id, esperar=conexion_broker.sleep)

            # 2C. Actualizar a PUBLICADO solo después de que RabbitMQ confirmó la recepción
            with transaccion_bd() as cursor:
                cursor.execute("UPDATE outbox_eventos SET estado = 'PUBLICADO' WHERE id = %s;", (evento_id,))

            log_flujo(calificacion_id, "PASO 2", "RELAY -> BD", f"Confirmado por Broker. Evento #{evento_id} actualizado a PUBLICADO.")

        conexion_broker.close()

    except (pika.exceptions.AMQPError, OSError) as error_broker:
        ids = ", ".join([f"#{fila[1]}" for fila in eventos_pendientes])
        print(f"[CALIFICACIÓN {ids} | PASO 2 - RELAY ALERTA] Broker caído ({type(error_broker).__name__}). "
              f"Eventos siguen en PENDIENTE. Reintentando en {ESPERA_SI_BROKER_CAIDO}s...")
        time.sleep(ESPERA_SI_BROKER_CAIDO)


if __name__ == "__main__":
    print("[INICIO RELAY M2] Proceso Outbox Relay activo (sondeo cada 1s)...")
    while True:
        try:
            limpiar_pausa("relay")
            break
        except psycopg2.Error:
            time.sleep(2)
    while True:
        try:
            retransmitir_eventos_outbox()
        except psycopg2.Error as error_bd:
            print(f"[RELAY - ALERTA] Sin conexión con PostgreSQL ({type(error_bd).__name__}). Reintentando...")
            time.sleep(2)
        time.sleep(1)
