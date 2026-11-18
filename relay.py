import time
import pika
from comun import COLA_M2, transaccion_bd, conectar_broker, log_flujo

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
            log_flujo(calificacion_id, "PASO 2", "RELAY -> BROKER", f"Publicando Evento #{evento_id} en '{COLA_M2}'...")

            canal.basic_publish(
                exchange="", routing_key=COLA_M2, body=payload_json,
                properties=pika.BasicProperties(delivery_mode=2, content_type="application/json")
            )

            # 2C. Actualizar a PUBLICADO solo después de que RabbitMQ confirmó la recepción
            with transaccion_bd() as cursor:
                cursor.execute("UPDATE outbox_eventos SET estado = 'PUBLICADO' WHERE id = %s;", (evento_id,))

            log_flujo(calificacion_id, "PASO 2", "RELAY -> BD", f"Confirmado por Broker. Evento #{evento_id} actualizado a PUBLICADO.")

        conexion_broker.close()

    except (pika.exceptions.AMQPError, OSError) as error_broker:
        ids = ", ".join([f"#{fila[1]}" for fila in eventos_pendientes])
        print(f"[CALIFICACIÓN {ids} | PASO 2 - RELAY ALERTA] Broker caído ({type(error_broker).__name__}). Eventos siguen en PENDIENTE. Reintentando en 10s...")
        time.sleep(9)

if __name__ == "__main__":
    print("[INICIO RELAY M2] Proceso Outbox Relay activo (sondeo cada 1s)...")
    while True:
        retransmitir_eventos_outbox()
        time.sleep(1)