import json
import pika
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from comun import COLA_M2, transaccion_bd, conectar_broker, log_flujo

app = FastAPI(title="TPI Módulo M2 (Clientes) - RF-2.5: Calificación del Conductor")

class SolicitudCalificar(BaseModel):
    viaje_id: str = Field(default="viaje_101")
    cliente_id: str = Field(default="cliente_matias")
    conductor_id: str = Field(default="conductor_carlos")
    puntaje: int = Field(default=5, ge=1, le=5)
    comentario: str = Field(default="Excelente conducción y puntualidad")

# =====================================================================
# 1. FLUJO PRINCIPAL CON PATRÓN TRANSACTIONAL OUTBOX (PASO 1)
# =====================================================================
@app.post("/calificaciones", status_code=status.HTTP_202_ACCEPTED, summary="1. Registrar calificación CON Outbox (Seguro)")
def registrar_calificacion(solicitud: SolicitudCalificar):
    print(f"\n[PASO 1 - HTTP -> API M2] Solicitud POST recibida de '{solicitud.cliente_id}' ({solicitud.puntaje} estrellas).")
    try:
        # El bloque 'with' garantiza que ambos INSERT compartan un único COMMIT o ROLLBACK
        with transaccion_bd() as cursor:
            # 1A. Guardar entidad de negocio en tabla 'calificaciones'
            cursor.execute(
                """
                INSERT INTO calificaciones (viaje_id, cliente_id, conductor_id, puntaje, comentario, estado)
                VALUES (%s, %s, %s, %s, %s, 'PENDIENTE_IMPACTO') RETURNING id;
                """,
                (solicitud.viaje_id, solicitud.cliente_id, solicitud.conductor_id, solicitud.puntaje, solicitud.comentario)
            )
            calificacion_id = cursor.fetchone()[0]

            # 1B. Guardar evento en tabla 'outbox_eventos' en la MISMA transacción
            payload = json.dumps({"calificacion_id": calificacion_id, **solicitud.model_dump()})
            cursor.execute(
                """
                INSERT INTO outbox_eventos (calificacion_id, tipo_evento, payload, estado)
                VALUES (%s, 'ConductorCalificado', %s, 'PENDIENTE') RETURNING id;
                """,
                (calificacion_id, payload)
            )
            evento_id = cursor.fetchone()[0]

        log_flujo(calificacion_id, "PASO 1", "API M2 -> BD", f"COMMIT atómico exitoso: 'calificaciones' y 'outbox_eventos' (Evento #{evento_id} en PENDIENTE).")
        return {
            "mensaje": "Calificación registrada con Outbox. Se procesará asíncronamente.",
            "calificacion_id": calificacion_id,
            "evento_outbox_id": evento_id,
            "estado_actual": "PENDIENTE_IMPACTO"
        }
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Fallo en BD (se aplicó ROLLBACK automático): {error}")

# =====================================================================
# 2. CONTRAPARTE SIN OUTBOX (DEMUESTRA PROBLEMA DUAL-WRITE)
# =====================================================================
@app.post("/calificaciones/sin-outbox", status_code=status.HTTP_202_ACCEPTED, summary="2. Registrar calificación SIN Outbox (Falla si cae el Broker)")
def registrar_sin_outbox(solicitud: SolicitudCalificar):
    with transaccion_bd() as cursor:
        cursor.execute(
            """
            INSERT INTO calificaciones (viaje_id, cliente_id, conductor_id, puntaje, comentario, estado)
            VALUES (%s, %s, %s, %s, %s, 'PENDIENTE_IMPACTO') RETURNING id;
            """,
            (solicitud.viaje_id, solicitud.cliente_id, solicitud.conductor_id, solicitud.puntaje, solicitud.comentario)
        )
        calificacion_id = cursor.fetchone()[0]

    log_flujo(calificacion_id, "SIN OUTBOX", "API M2 -> BD", "Tabla 'calificaciones' guardada con COMMIT directo.")

    try:
        conexion_broker, canal = conectar_broker()
        payload = json.dumps({"calificacion_id": calificacion_id, **solicitud.model_dump()})
        canal.basic_publish(
            exchange="", routing_key=COLA_M2, body=payload,
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json")
        )
        conexion_broker.close()
        return {"mensaje": "Enviado directo a RabbitMQ (Sin Outbox).", "calificacion_id": calificacion_id}
    except (pika.exceptions.AMQPError, OSError) as error_broker:
        log_flujo(calificacion_id, "SIN OUTBOX", "INCONSISTENCIA", f"RabbitMQ caído ({type(error_broker).__name__}). Calificación #{calificacion_id} huérfana en BD.")
        raise HTTPException(status_code=500, detail=f"Inconsistencia Dual-Write: Calificación #{calificacion_id} guardada en BD pero el evento hacia M3 se perdió.")

# =====================================================================
# 3. ENDPOINTS DE PRUEBA DE IDEMPOTENCIA Y MONITOREO
# =====================================================================
@app.post("/calificaciones/{calificacion_id}/forzar-duplicado", summary="3. Reenviar evento (Demuestra Idempotencia RF-2.5 / RNF-09)")
def forzar_duplicado(calificacion_id: int):
    with transaccion_bd() as cursor:
        cursor.execute("UPDATE outbox_eventos SET estado = 'PENDIENTE' WHERE calificacion_id = %s RETURNING id;", (calificacion_id,))
        fila = cursor.fetchone()
    if not fila:
        raise HTTPException(status_code=404, detail="Evento Outbox no encontrado.")
    log_flujo(calificacion_id, "DUPLICADO", "API M2 -> BD", f"Evento #{fila[0]} devuelto a PENDIENTE para forzar segundo envío.")
    return {"mensaje": f"Evento #{fila[0]} de Calificación #{calificacion_id} reencolado para probar idempotencia."}

@app.get("/resumen", summary="4. Resumen de BD y Cola RabbitMQ en tiempo real")
def resumen_sistema():
    with transaccion_bd() as cursor:
        cursor.execute("SELECT estado, COUNT(*) FROM calificaciones GROUP BY estado;")
        calificaciones = dict(cursor.fetchall())
        cursor.execute("SELECT estado, COUNT(*) FROM outbox_eventos GROUP BY estado;")
        outbox = dict(cursor.fetchall())
        cursor.execute("SELECT id FROM calificaciones WHERE id NOT IN (SELECT calificacion_id FROM outbox_eventos) ORDER BY id;")
        huerfanas = [f[0] for f in cursor.fetchall()]

    try:
        conexion_broker, canal = conectar_broker(timeout=2)
        estado_cola = canal.queue_declare(queue=COLA_M2, durable=True, passive=True)
        info_rabbit = f"{estado_cola.method.message_count} mensaje(s) en estado Ready"
        conexion_broker.close()
    except Exception:
        info_rabbit = "Broker RabbitMQ APAGADO"

    return {
        "tabla_calificaciones_m2": calificaciones,
        "tabla_outbox_eventos_m2": outbox,
        "cola_rabbitmq_en_vivo": info_rabbit,
        "calificaciones_huerfanas_sin_outbox": huerfanas
    }

@app.get("/calificaciones/{calificacion_id}", summary="5. Consultar detalle de una calificación y su Outbox")
def consultar_calificacion(calificacion_id: int):
    with transaccion_bd() as cursor:
        cursor.execute("SELECT id, viaje_id, cliente_id, conductor_id, puntaje, estado FROM calificaciones WHERE id = %s;", (calificacion_id,))
        calif = cursor.fetchone()
        if not calif:
            raise HTTPException(status_code=404, detail="No encontrada")
        cursor.execute("SELECT id, tipo_evento, estado, payload FROM outbox_eventos WHERE calificacion_id = %s;", (calificacion_id,))
        outbox = cursor.fetchone()

    return {
        "calificacion": {"id": calif[0], "viaje_id": calif[1], "cliente": calif[2], "conductor": calif[3], "puntaje": calif[4], "estado": calif[5]},
        "outbox": {"evento_id": outbox[0], "tipo": outbox[1], "estado": outbox[2], "payload": json.loads(outbox[3])} if outbox else "INCONSISTENTE (Sin Outbox)"
    }