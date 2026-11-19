import base64
import json
import os
import urllib.request

import pika
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from comun import BROKER_HOST, COLA_M2, transaccion_bd, conectar_broker, log_flujo, generacion_actual

app = FastAPI(title="TPI Módulo M2 (Clientes) - RF-2.5: Calificación del Conductor")

PANEL_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "panel.html")


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
                INSERT INTO calificaciones (viaje_id, cliente_id, conductor_id, puntaje, comentario)
                VALUES (%s, %s, %s, %s, %s) RETURNING id;
                """,
                (solicitud.viaje_id, solicitud.cliente_id, solicitud.conductor_id, solicitud.puntaje, solicitud.comentario)
            )
            calificacion_id = cursor.fetchone()[0]

            # 1B. Guardar evento en tabla 'outbox_eventos' en la MISMA transacción
            payload = json.dumps({"calificacion_id": calificacion_id, **solicitud.model_dump(),
                                  "generacion": generacion_actual(cursor)})
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
            INSERT INTO calificaciones (viaje_id, cliente_id, conductor_id, puntaje, comentario)
            VALUES (%s, %s, %s, %s, %s) RETURNING id;
            """,
            (solicitud.viaje_id, solicitud.cliente_id, solicitud.conductor_id, solicitud.puntaje, solicitud.comentario)
        )
        calificacion_id = cursor.fetchone()[0]
        generacion = generacion_actual(cursor)

    log_flujo(calificacion_id, "SIN OUTBOX", "API M2 -> BD", "Tabla 'calificaciones' guardada con COMMIT directo.")

    try:
        conexion_broker, canal = conectar_broker()
        payload = json.dumps({"evento_id": f"sin-outbox-{calificacion_id}", "calificacion_id": calificacion_id, **solicitud.model_dump(),
                              "generacion": generacion})
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
@app.post("/calificaciones/{calificacion_id}/forzar-duplicado", summary="3. Reenviar evento (Demuestra Idempotencia del consumidor)")
def forzar_duplicado(calificacion_id: int):
    with transaccion_bd() as cursor:
        cursor.execute("UPDATE outbox_eventos SET estado = 'PENDIENTE' WHERE calificacion_id = %s RETURNING id;", (calificacion_id,))
        fila = cursor.fetchone()
    if not fila:
        raise HTTPException(status_code=404, detail="Evento Outbox no encontrado.")
    log_flujo(calificacion_id, "DUPLICADO", "API M2 -> BD", f"Evento #{fila[0]} devuelto a PENDIENTE para forzar segundo envío.")
    return {"mensaje": f"Evento #{fila[0]} de Calificación #{calificacion_id} reencolado para probar idempotencia."}


def estado_cola():
    """Mensajes Ready y Unacked de la cola, leídos de la API de administración de RabbitMQ."""
    try:
        pedido = urllib.request.Request(f"http://{BROKER_HOST}:15672/api/queues/%2F/{COLA_M2}")
        pedido.add_header("Authorization", "Basic " + base64.b64encode(b"guest:guest").decode())
        with urllib.request.urlopen(pedido, timeout=1.5) as respuesta:
            datos = json.loads(respuesta.read())
        return {"disponible": True, "ready": datos.get("messages_ready", 0), "unacked": datos.get("messages_unacknowledged", 0)}
    except Exception:
        pass
    try:  # Si la consola de administración no responde, al menos se cuentan los Ready
        conexion_broker, canal = conectar_broker(timeout=1.5)
        ready = canal.queue_declare(queue=COLA_M2, durable=True, passive=True).method.message_count
        conexion_broker.close()
        return {"disponible": True, "ready": ready, "unacked": None}
    except Exception:
        return {"disponible": False, "ready": None, "unacked": None}


def leer_estado():
    with transaccion_bd() as cursor:
        cursor.execute("SELECT COUNT(*) FROM calificaciones;")
        guardadas = cursor.fetchone()[0]
        cursor.execute("SELECT estado, COUNT(*) FROM outbox_eventos GROUP BY estado;")
        outbox = dict(cursor.fetchall())
        cursor.execute("SELECT id, calificacion_id, estado FROM outbox_eventos ORDER BY id DESC LIMIT 12;")
        ultimos = [{"evento_id": f[0], "calificacion_id": f[1], "estado": f[2]} for f in cursor.fetchall()]
        cursor.execute("SELECT id FROM calificaciones WHERE id NOT IN (SELECT calificacion_id FROM outbox_eventos) ORDER BY id;")
        huerfanas = [f[0] for f in cursor.fetchall()]
        cursor.execute("SELECT conductor_id, cantidad, suma FROM reputacion_conductor ORDER BY conductor_id;")
        reputacion = [{"conductor_id": f[0], "cantidad": f[1], "promedio": round(f[2] / f[1], 2)} for f in cursor.fetchall()]
        cursor.execute("SELECT COUNT(*) FROM eventos_procesados;")
        procesados = cursor.fetchone()[0]
        cursor.execute("SELECT paso_a_paso FROM demo_control WHERE id = 1;")
        paso_a_paso = cursor.fetchone()[0]
        cursor.execute("SELECT servicio, numero, descripcion, calificacion_id FROM demo_pausas WHERE NOT avanzar ORDER BY desde;")
        pausas = [{"servicio": f[0], "numero": f[1], "descripcion": f[2], "calificacion_id": f[3]} for f in cursor.fetchall()]
    return {
        "m2": {"calificaciones_guardadas": guardadas, "outbox": outbox, "ultimos_eventos": ultimos,
               "calificaciones_huerfanas_sin_outbox": huerfanas},
        "rabbitmq": estado_cola(),
        "m3": {"calificaciones_contadas": sum(r["cantidad"] for r in reputacion), "reputacion": reputacion,
               "eventos_procesados": procesados},
        "paso_a_paso": paso_a_paso,
        "pausas": pausas,
    }


@app.get("/resumen", summary="4. Resumen de M2, RabbitMQ y M3 en tiempo real")
def resumen_sistema():
    return leer_estado()


@app.get("/calificaciones/{calificacion_id}", summary="5. Consultar detalle de una calificación y su Outbox")
def consultar_calificacion(calificacion_id: int):
    with transaccion_bd() as cursor:
        cursor.execute("SELECT id, viaje_id, cliente_id, conductor_id, puntaje FROM calificaciones WHERE id = %s;", (calificacion_id,))
        calif = cursor.fetchone()
        if not calif:
            raise HTTPException(status_code=404, detail="No encontrada")
        cursor.execute("SELECT id, tipo_evento, estado, payload FROM outbox_eventos WHERE calificacion_id = %s;", (calificacion_id,))
        outbox = cursor.fetchone()

    return {
        "calificacion": {"id": calif[0], "viaje_id": calif[1], "cliente": calif[2], "conductor": calif[3], "puntaje": calif[4]},
        "outbox": {"evento_id": outbox[0], "tipo": outbox[1], "estado": outbox[2], "payload": json.loads(outbox[3])} if outbox else "INCONSISTENTE (Sin Outbox)"
    }


# =====================================================================
# 4. MODO PASO A PASO Y PANEL (solo para la exposición)
# =====================================================================
@app.get("/panel", include_in_schema=False)
def panel():
    return FileResponse(PANEL_HTML, media_type="text/html")


@app.post("/demo/paso-a-paso", summary="6. Activar o desactivar el modo paso a paso")
def cambiar_paso_a_paso(activo: bool):
    with transaccion_bd() as cursor:
        cursor.execute("UPDATE demo_control SET paso_a_paso = %s WHERE id = 1;", (activo,))
    print(f"[DEMO] Modo paso a paso {'ACTIVADO' if activo else 'DESACTIVADO'}.")
    return {"paso_a_paso": activo}


@app.post("/demo/avanzar", summary="7. Avanzar: libera la pausa más antigua (o la del servicio indicado)")
def avanzar(servicio: str | None = None):
    with transaccion_bd() as cursor:
        cursor.execute(
            """
            UPDATE demo_pausas SET avanzar = TRUE
             WHERE servicio = (SELECT servicio FROM demo_pausas
                                WHERE NOT avanzar AND (%s IS NULL OR servicio = %s)
                                ORDER BY desde LIMIT 1)
            RETURNING servicio, numero;
            """,
            (servicio, servicio),
        )
        fila = cursor.fetchone()
    if not fila:
        raise HTTPException(status_code=409, detail="No hay ningún servicio esperando en una pausa.")
    return {"avanzo": fila[0], "pausa": fila[1]}


@app.post("/demo/reiniciar", summary="8. Empezar de cero: borra todos los datos de la demo y vacía la cola")
def reiniciar_demo():
    with transaccion_bd() as cursor:
        cursor.execute("TRUNCATE calificaciones, outbox_eventos, reputacion_conductor, eventos_procesados, demo_pausas RESTART IDENTITY;")
        # Los mensajes que estaban en camino (por ejemplo, uno que el worker tenía en la mano)
        # quedan con la generación anterior y el worker los descarta.
        cursor.execute("UPDATE demo_control SET paso_a_paso = FALSE, generacion = generacion + 1 WHERE id = 1;")
    try:
        conexion_broker, canal = conectar_broker(timeout=2)
        canal.queue_purge(queue=COLA_M2)
        conexion_broker.close()
        cola = "vaciada"
    except Exception:
        cola = "no se pudo vaciar (RabbitMQ caído); sus mensajes viejos se descartan al llegar"
    print(f"\n[DEMO] EMPEZAR DE CERO: datos borrados, cola {cola}.\n")
    return {"mensaje": "Demo reiniciada.", "cola": cola}
