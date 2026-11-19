import time
import pika
from comun import COLA_M2, transaccion_bd, conectar_broker, log_flujo

def retransmitir_eventos_outbox():
    conexion_broker = None
    try:
        # 2A. Consultar eventos pendientes únicamente en el esquema de M2
        with transaccion_bd() as cursor:
            cursor.execute("SELECT id, calif_id, payload FROM m2.outbox_eventos WHERE estado = 'PENDIENTE' ORDER BY id ASC;")
            eventos_pendientes = cursor.fetchall()

        if not eventos_pendientes:
            return

        # 2B. Abrir conexión con RabbitMQ y publicar con confirmación del broker
        conexion_broker, canal = conectar_broker()
        canal.confirm_delivery()

        for evento_id, calificacion_id, payload_json in eventos_pendientes:
            log_flujo(calificacion_id, "PASO 2", "RELAY M2 <- BD M2", f"Evento #{evento_id} leído en estado PENDIENTE.")
            log_flujo(calificacion_id, "PASO 2", "RELAY M2 -> BROKER", f"Publicando Evento #{evento_id} en '{COLA_M2}'...")

            canal.basic_publish(
                exchange="", routing_key=COLA_M2, body=payload_json,
                properties=pika.BasicProperties(delivery_mode=2, content_type="application/json")
            )

            # 2C. Marcar como PUBLICADO solo después de la confirmación del broker
            with transaccion_bd() as cursor:
                cursor.execute("UPDATE m2.outbox_eventos SET estado = 'PUBLICADO' WHERE id = %s;", (evento_id,))

            log_flujo(calificacion_id, "PASO 2", "RELAY M2 -> BD M2", f"Confirmado por Broker. Evento #{evento_id} actualizado a PUBLICADO.")

    except Exception as error:
        print(f"[RELAY M2 - ALERTA] Fallo temporal ({type(error).__name__}: {error}). Reintentando en 3s...")
        time.sleep(3)
    finally:
        if conexion_broker and conexion_broker.is_open:
            conexion_broker.close()

if __name__ == "__main__":
    print("[INICIO RELAY M2] Proceso Outbox Relay activo y blindado (sondeo cada 1s)...")
    while True:
        retransmitir_eventos_outbox()
        time.sleep(1)