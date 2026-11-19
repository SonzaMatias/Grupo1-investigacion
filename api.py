import json
import pika
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from comun import COLA_M2, COLA_DLQ, transaccion_bd, conectar_broker, log_flujo

app = FastAPI(title="TPI Módulo M2 (Clientes) - Outbox, Inbox Atómico y DLQ")

class SolicitudCalificar(BaseModel):
    viaje: str = Field(default="v_101")
    cliente: str = Field(default="cli_1")
    conductor: str = Field(default="cond_1")
    puntaje: int = Field(default=5, ge=1, le=5)

# =====================================================================
# 1. FLUJO PRINCIPAL CON PATRÓN TRANSACTIONAL OUTBOX (PASO 1 - M2)
# =====================================================================
@app.post("/calificaciones", status_code=status.HTTP_202_ACCEPTED, summary="1. Registrar calificación CON Outbox (Seguro)")
def registrar_calificacion(solicitud: SolicitudCalificar):
    print(f"[PASO 1 - HTTP -> API M2] Solicitud POST recibida de '{solicitud.cliente}' ({solicitud.puntaje} estrellas).")
    try:
        with transaccion_bd() as cursor:
            # 1A. Guardar entidad de negocio en el esquema de M2
            cursor.execute(
                """
                INSERT INTO m2.calificaciones (viaje, cliente, conductor, puntaje)
                VALUES (%s, %s, %s, %s) RETURNING id;
                """,
                (solicitud.viaje, solicitud.cliente, solicitud.conductor, solicitud.puntaje)
            )
            calificacion_id = cursor.fetchone()[0]

            # 1B. Evento mínimo (Thin Event) sin espacios extra: ~47 caracteres
            payload = json.dumps(
                {"calif_id": calificacion_id, "conductor": solicitud.conductor, "puntaje": solicitud.puntaje},
                separators=(",", ":")
            )
            cursor.execute(
                """
                INSERT INTO m2.outbox_eventos (calif_id, evento, payload, estado)
                VALUES (%s, 'ConductorCalificado', %s, 'PENDIENTE') RETURNING id;
                """,
                (calificacion_id, payload)
            )
            evento_id = cursor.fetchone()[0]

        log_flujo(calificacion_id, "PASO 1", "API M2 -> BD M2", f"COMMIT atómico: 'm2.calificaciones' y 'm2.outbox_eventos' (Evento #{evento_id} en PENDIENTE).")
        return {
            "mensaje": "Calificación registrada en M2 con Outbox. Se procesará asíncronamente en M3.",
            "calificacion_id": calificacion_id,
            "evento_outbox_id": evento_id
        }
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Fallo en BD M2 (se aplicó ROLLBACK automático): {error}")

# =====================================================================
# 2. CONTRAPARTE SIN OUTBOX (DEMUESTRA PROBLEMA DUAL-WRITE)
# =====================================================================
@app.post("/calificaciones/sin-outbox", status_code=status.HTTP_202_ACCEPTED, summary="2. Registrar calificación SIN Outbox (Falla si cae el Broker)")
def registrar_sin_outbox(solicitud: SolicitudCalificar):
    with transaccion_bd() as cursor:
        cursor.execute(
            """
            INSERT INTO m2.calificaciones (viaje, cliente, conductor, puntaje)
            VALUES (%s, %s, %s, %s) RETURNING id;
            """,
            (solicitud.viaje, solicitud.cliente, solicitud.conductor, solicitud.puntaje)
        )
        calificacion_id = cursor.fetchone()[0]

    log_flujo(calificacion_id, "SIN OUTBOX", "API M2 -> BD M2", "Tabla 'm2.calificaciones' guardada con COMMIT directo.")

    try:
        conexion_broker, canal = conectar_broker()
        payload = json.dumps(
            {"calif_id": calificacion_id, "conductor": solicitud.conductor, "puntaje": solicitud.puntaje},
            separators=(",", ":")
        )
        canal.basic_publish(
            exchange="", routing_key=COLA_M2, body=payload,
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json")
        )
        conexion_broker.close()
        return {"mensaje": "Enviado directo a RabbitMQ (Sin Outbox).", "calificacion_id": calificacion_id}
    except (pika.exceptions.AMQPError, OSError) as error_broker:
        log_flujo(calificacion_id, "SIN OUTBOX", "INCONSISTENCIA", f"RabbitMQ caído ({type(error_broker).__name__}). Calificación #{calificacion_id} huérfana en M2.")
        raise HTTPException(status_code=500, detail=f"Inconsistencia Dual-Write: Calificación #{calificacion_id} guardada en M2 pero el evento hacia M3 se perdió.")

# =====================================================================
# 3. PRUEBAS DE IDEMPOTENCIA (INBOX), POISON PILL (DLQ) Y MONITOREO
# =====================================================================
@app.post("/calificaciones/{calificacion_id}/forzar-duplicado", summary="3. Reenviar evento (Demuestra Inbox Atómico / Idempotencia)")
def forzar_duplicado(calificacion_id: int):
    with transaccion_bd() as cursor:
        cursor.execute("UPDATE m2.outbox_eventos SET estado = 'PENDIENTE' WHERE calif_id = %s RETURNING id;", (calificacion_id,))
        fila = cursor.fetchone()
    if not fila:
        raise HTTPException(status_code=404, detail="Evento Outbox no encontrado en M2.")
    log_flujo(calificacion_id, "DUPLICADO", "API M2 -> BD M2", f"Evento #{fila[0]} devuelto a PENDIENTE para forzar segundo envío.")
    return {"mensaje": f"Evento #{fila[0]} de Calificación #{calificacion_id} reencolado para probar idempotencia en M3."}

@app.post("/calificaciones/enviar-corrupto", summary="4. Inyectar JSON corrupto (Demuestra Poison Pill -> DLQ)")
def enviar_mensaje_corrupto():
    try:
        conexion_broker, canal = conectar_broker()
        payload_roto = "{calificacion_id: SIN_COMILLAS_Y_SIN_CERRAR"
        canal.basic_publish(
            exchange="", routing_key=COLA_M2, body=payload_roto,
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json")
        )
        conexion_broker.close()
        print("[PRUEBA DLQ - API -> BROKER] Mensaje corrupto inyectado en la cola principal.")
        return {"mensaje": "Mensaje corrupto enviado a RabbitMQ. El Worker M3 debe rechazarlo sin reencolar (hacia DLQ)."}
    except (pika.exceptions.AMQPError, OSError) as error:
        raise HTTPException(status_code=500, detail=f"No se pudo conectar al broker: {error}")

@app.get("/resumen", summary="5. Panel de monitoreo en vivo (BD M2, BD M3, Cola y DLQ)")
def resumen_sistema():
    # Nota: este endpoint solo lee ambos esquemas con fines de observabilidad para la demo.
    with transaccion_bd() as cursor:
        cursor.execute("SELECT COUNT(*) FROM m2.calificaciones;")
        total_m2 = cursor.fetchone()[0]
        cursor.execute("SELECT estado, COUNT(*) FROM m2.outbox_eventos GROUP BY estado;")
        outbox_m2 = dict(cursor.fetchall())
        cursor.execute("SELECT id FROM m2.calificaciones WHERE id NOT IN (SELECT calificacion_id FROM m2.outbox_eventos) ORDER BY id;")
        huerfanas = [f[0] for f in cursor.fetchall()]

        cursor.execute("SELECT id FROM m2.calificaciones WHERE id NOT IN (SELECT calif_id FROM m2.outbox_eventos) ORDER BY id;")
        huerfanas = [f[0] for f in cursor.fetchall()]

        cursor.execute("SELECT calif_id FROM m3.inbox_eventos ORDER BY calif_id;")
        inbox_m3 = [f[0] for f in cursor.fetchall()]
        cursor.execute("SELECT conductor, cant, suma, promedio FROM m3.conductores_reputacion ORDER BY conductor;")
        reputacion_m3 = [
            {"conductor": f[0], "cant": f[1], "suma": f[2], "promedio": float(f[3])}
            for f in cursor.fetchall()
        ]

    try:
        conexion_broker, canal = conectar_broker(timeout=2)
        estado_cola = canal.queue_declare(queue=COLA_M2, passive=True)
        estado_dlq = canal.queue_declare(queue=COLA_DLQ, passive=True)
        info_rabbit = {
            COLA_M2: f"{estado_cola.method.message_count} mensaje(s) Ready",
            COLA_DLQ: f"{estado_dlq.method.message_count} mensaje(s) muertos (DLQ)"
        }
        conexion_broker.close()
    except Exception:
        info_rabbit = "Broker RabbitMQ APAGADO"

    return {
        "esquema_m2_clientes": {
            "total_calificaciones": total_m2,
            "outbox_eventos": outbox_m2,
            "huerfanas_sin_outbox": huerfanas
        },
        "esquema_m3_conductores": {
            "inbox_procesados_ids": inbox_m3,
            "reputacion_conductores": reputacion_m3
        },
        "rabbitmq_en_vivo": info_rabbit
    }

@app.get("/calificaciones/{calificacion_id}", summary="6. Consultar detalle de una calificación en M2 y su impacto en M3")
def consultar_calificacion(calificacion_id: int):
    # En consultar_calificacion():
    with transaccion_bd() as cursor:
        cursor.execute("SELECT id, viaje, cliente, conductor, puntaje, hora FROM m2.calificaciones WHERE id = %s;", (calificacion_id,))
        calif = cursor.fetchone()
        if not calif:
            raise HTTPException(status_code=404, detail="No encontrada en M2")
        cursor.execute("SELECT id, evento, estado, payload FROM m2.outbox_eventos WHERE calif_id = %s;", (calificacion_id,))
        outbox = cursor.fetchone()
        cursor.execute("SELECT procesado FROM m3.inbox_eventos WHERE calif_id = %s;", (calificacion_id,))
        inbox = cursor.fetchone()

    return {
        "m2_calificacion": {"id": calif[0], "viaje": calif[1], "cliente": calif[2], "conductor": calif[3], "puntaje": calif[4], "hora": str(calif[5])},
        "m2_outbox": {"evento_id": outbox[0], "evento": outbox[1], "estado": outbox[2], "payload": json.loads(outbox[3])} if outbox else "INCONSISTENTE (Sin Outbox)",
        "m3_inbox": f"IMPACTADA en M3 ({inbox[0]})" if inbox else "Aún no procesada por M3"
    }