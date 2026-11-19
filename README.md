# Módulo M2 (Clientes) - Calificación de Conductores con Transactional Outbox y Colas Asíncronas

# Arquitectura Asíncrona Tolerante a Fallos: Transactional Outbox, Inbox Atómico y DLQ

Implementación de referencia de una arquitectura distribuida desacoplada para la comunicación entre el **Módulo M2 (Clientes)** y el **Módulo M3 (Conductores)** al registrar calificaciones de viajes (**RF-2.5**), garantizando aislamiento de datos (**RNF-05**), tolerancia a fallos de infraestructura, procesamiento concurrente seguro e idempotencia (**RNF-09**).

**Stack tecnológico:** Python 3.11 (FastAPI, Pydantic, Pika, Psycopg2), PostgreSQL (esquemas lógicos aislados `m2` y `m3`), RabbitMQ y Docker Compose.

---

## 1. Fundamento Teórico: Evolución de la Arquitectura

El diseño del sistema responde paso a paso a los problemas clásicos de la comunicación entre microservicios.

### 1.1 El problema de la comunicación síncrona (HTTP directo)
En un enfoque ingenuo (`Cliente -> API M2 -> API M3 -> BD M3`), M2 invoca directamente a M3 y espera su respuesta antes de contestarle al usuario. Esto genera cuatro vulnerabilidades críticas:
* **Latencia acumulada y bloqueos:** Si M3 realiza cálculos costosos (reputación histórica, reglas de suspensión, auditoría), el cliente queda bloqueado esperando hasta sufrir un *timeout*.
* **Acoplamiento temporal (fallos en cascada):** Si M3 se cae o entra en mantenimiento, M2 devuelve error `500` y no puede registrar calificaciones aunque su propia base de datos esté operativa.
* **Vulnerabilidad ante picos de carga:** Ráfagas de viajes finalizando simultáneamente saturan hilos y conexiones en M2 y M3.
* **Reintentos duplicados:** Ante una demora, el cliente puede reenviar la solicitud HTTP y computar la misma calificación múltiples veces.

### 1.2 Asincronismo, Nivelación de Carga y el Problema *Dual-Write*
Introducir un broker de mensajería (**RabbitMQ**) desacopla en el tiempo a M2 y M3:
* **Asynchronous Request-Reply:** La API de M2 responde inmediatamente **`HTTP 202 Accepted`** tras persistir la solicitud, liberando al cliente.
* **Queue-Based Load Leveling:** La cola actúa como amortiguador ante picos de tráfico; los consumidores procesan a un ritmo constante y controlado (`prefetch_count=1`).

Sin embargo, intentar escribir directamente en la base de datos y publicar en RabbitMQ dentro del mismo endpoint (`POST /calificaciones/sin-outbox`) introduce el **Problema Dual-Write**: PostgreSQL y RabbitMQ son sistemas independientes que **no comparten una transacción ACID**.
* Si se hace `COMMIT` en PostgreSQL y luego falla la publicación porque RabbitMQ está caído, la calificación queda guardada en M2 pero **el evento se pierde para siempre** (dato huérfano e inconsistencia permanente con M3).
* Invertir el orden tampoco lo resuelve: si se publica en RabbitMQ primero y falla el `COMMIT` en PostgreSQL, M3 procesaría una calificación inexistente.

### 1.3 Transactional Outbox y Polling Publisher
Para eliminar el *Dual-Write* sin transacciones distribuidas costosas (*Two-Phase Commit*):
1. **Transactional Outbox (`api.py`):** Dentro de **una única transacción local ACID** (`with transaccion_bd()`), M2 inserta la calificación en `m2.calificaciones` y el evento a emitir en `m2.outbox_eventos` con estado `'PENDIENTE'`. Ambas escrituras se confirman juntas (`COMMIT`) o se revierten juntas (`ROLLBACK`). La API **nunca interactúa con RabbitMQ**.
2. **Polling Publisher (`relay.py`):** Un proceso independiente sondea periódicamente `m2.outbox_eventos` buscando registros `'PENDIENTE'`, los publica en RabbitMQ con persistencia en disco (`delivery_mode=2`) y confirmación del broker (`confirm_delivery()`), y **solo tras recibir dicha confirmación** actualiza el registro a `'PUBLICADO'`.

### 1.4 Aislamiento de Datos (*Database per Service*) y Eventos Mínimos (*Thin Events*)
* **Esquemas separados (`m2` y `m3`):** M2 (`api.py` y `relay.py`) opera exclusivamente sobre `m2.*`; M3 (`worker.py`) opera exclusivamente sobre `m3.*`. M3 jamás lee ni actualiza tablas de M2.
* **Minimización de datos:** En lugar de replicar toda la entidad en el evento, M2 publica únicamente el contrato mínimo necesario para M3 (~47 bytes):
  ```json
  {"calif_id":1,"conductor":"cond_1","puntaje":5}
  ```

### 1.5 Entrega *At-Least-Once*, Idempotencia y Concurrencia (Doble Filtro en M3)
El patrón Outbox garantiza entrega **al menos una vez (*At-Least-Once*)**: si el Relay muere en el instante exacto entre publicar en RabbitMQ y ejecutar `UPDATE ... SET estado = 'PUBLICADO'`, al reiniciarse reenviará el mismo evento. Asimismo, si un Worker cae antes de emitir el `ACK` manual (`auto_ack=False`), RabbitMQ reentrega el mensaje (`redelivered=True`).

Para garantizar efecto **exactamente una vez** incluso con múltiples workers escalados en paralelo (`--scale worker=2`) y evitar que workers queden bloqueados esperando duplicados, `worker.py` aplica un **doble filtro dentro de la misma transacción en M3**:
1. **Filtro 1 — Concurrencia en vuelo (`pg_try_advisory_xact_lock(calif_id)`):** Candado consultivo no bloqueante atado a la transacción de PostgreSQL. Si otro worker ya está procesando ese `calif_id`, devuelve `False` en menos de 1 ms. El segundo worker libera la conexión SQL, descarta la copia redundante con `ACK` y queda inmediatamente disponible para atender otros mensajes sin pausarse. *(Es seguro confirmar la copia sobrante porque la existencia del candado garantiza que el primer worker aún retiene el mensaje original en estado `Unacked` en RabbitMQ; si muriera, RabbitMQ lo reentregaría y PostgreSQL liberaría el candado vía `ROLLBACK`).*
2. **Filtro 2 — Idempotencia histórica (`Transactional Inbox` en `m3.inbox_eventos`):** Ejecuta `INSERT INTO m3.inbox_eventos (calif_id) VALUES (%s) ON CONFLICT (calif_id) DO NOTHING RETURNING calif_id` dentro de la misma transacción que actualiza `m3.conductores_reputacion`. Si el `calif_id` ya fue procesado anteriormente, no retorna filas, se descarta con `ACK` y la reputación del conductor permanece intacta.

### 1.6 Mensajes Corruptos (*Poison Pills*) y Dead Letter Queue (DLQ)
Un *Poison Pill* es un mensaje con error permanente de formato o contrato (JSON mal formado, codificación inválida, claves faltantes o puntaje fuera de rango `1..5`). Si se reencolara con `basic_nack(..., requeue=True)`, entraría en un bucle infinito bloqueando la cola principal.

* **Solución:** Al declarar la cola principal `cola_calificaciones_m2` en `comun.py`, se enlaza a una cola de cuarentena durable **`cola_calificaciones_dlq`** mediante `x-dead-letter-exchange: ""` y `x-dead-letter-routing-key: "cola_calificaciones_dlq"`.
* **Separación de errores en `worker.py`:**
  * **Errores permanentes de contrato** (`UnicodeDecodeError`, `JSONDecodeError`, `KeyError`, `ValueError`, `TypeError`) $\rightarrow$ `basic_nack(..., requeue=False)`: RabbitMQ desvía automáticamente el mensaje a la **DLQ** para auditoría forense sin detener la cola principal.
  * **Errores transitorios de infraestructura** (ej. caída temporal de PostgreSQL) $\rightarrow$ pausa de 3s + `basic_nack(..., requeue=True)` para reintentar cuando el recurso vuelva.

---

## 2. Arquitectura y Estructura del Proyecto

### Diagrama de flujo

```text
  Cliente / Swagger / demo.ps1
              │
              │ HTTP POST (202 Accepted)
              ▼
         ┌─────────┐    COMMIT atómico     ┌─────────────────────────┐
         │ api.py  ├──────────────────────►│   PostgreSQL (m2.*)     │
         │  (M2)   │                       │ - m2.calificaciones     │
         └─────────┘                       │ - m2.outbox_eventos     │
                                           └───────────┬─────────────┘
                                                       │ Polling (1s) + UPDATE PUBLICADO
                                                       ▼
                                                 ┌───────────┐
                                                 │ relay.py  │
                                                 │   (M2)    │
                                                 └─────┬─────┘
                                                       │ basic_publish + confirm_delivery
                                                       ▼
                                           ┌───────────────────────────┐
                                           │         RabbitMQ          │
                                           │  cola_calificaciones_m2   │──┐ (requeue=False)
                                           └─────────────┬─────────────┘  │
                                                         │                ▼
                                 prefetch=1, auto_ack=False     ┌───────────────────────────┐
                                                         │      │ cola_calificaciones_dlq   │
                                                         ▼      │  (Cuarentena Poison Pill) │
                                              ┌────────────────────┐└───────────────────────┘
                                              │ worker.py (x1..xN) │
                                              │        (M3)        │
                                              └──────────┬─────────┘
                                                         │ pg_try_advisory_xact_lock
                                                         │ + COMMIT atómico -> ACK
                                                         ▼
                                           ┌───────────────────────────┐
                                           │    PostgreSQL (m3.*)      │
                                           │ - m3.inbox_eventos        │
                                           │ - m3.conductores_reputacion│
                                           └───────────────────────────┘
```

### Archivos del proyecto

| Archivo | Responsabilidad |
| :--- | :--- |
| **`init.sql`** | Inicializa los esquemas aislados `m2` y `m3`, tablas compactas, restricciones `CHECK` y claves primarias de idempotencia. |
| **`comun.py`** | Caja de herramientas compartida: gestor de contexto `transaccion_bd()` (`@contextmanager` con `COMMIT`/`ROLLBACK`/`finally`), `conectar_broker()` (declara cola principal durable y DLQ) y `log_flujo()` con *Correlation ID*. |
| **`api.py`** | Servicio HTTP FastAPI del Módulo M2. Implementa el registro atómico con Outbox, la contraparte insegura sin Outbox, endpoints de prueba (duplicado y JSON corrupto) y monitoreo (`/resumen`). |
| **`relay.py`** | *Polling Publisher* de M2. Sondea `m2.outbox_eventos` cada 1s y publica en RabbitMQ. Blindado con `try/except` global y `finally` para sobrevivir a caídas tanto de PostgreSQL como de RabbitMQ. |
| **`worker.py`** | Consumidor autónomo del Módulo M3 (escalable horizontalmente). Valida contrato (DLQ), aplica doble filtro de idempotencia/concurrencia, simula procesamiento pesado (40s en `Unacked`), actualiza reputación y envía `ACK` manual. |
| **`demo.ps1`** | Consola de control remoto en PowerShell para ejecutar todos los actos de demostración con una sola tecla sin modificar código Python. |
| **`docker-compose.yml`** | Orquesta los servicios `db`, `broker`, `api`, `relay` y `worker`. *(Asegurarse de que `worker` no tenga `container_name` fijo para permitir `--scale worker=2`).* |

### Modelo de datos compacto

| Esquema | Tabla | Columnas | Propósito |
| :--- | :--- | :--- | :--- |
| **`m2`** | `m2.calificaciones` | `id`, `viaje`, `cliente`, `conductor`, `puntaje`, `hora` (`TIME(0)`) | Entidad de negocio de M2. |
| **`m2`** | `m2.outbox_eventos` | `id`, `calif_id`, `evento`, `estado`, `payload` | Cola persistente local (`PENDIENTE` $\rightarrow$ `PUBLICADO`). |
| **`m3`** | `m3.inbox_eventos` | `calif_id` (`PK`), `procesado` (`TIME(0)`) | Registro atómico de eventos procesados por M3. |
| **`m3`** | `m3.conductores_reputacion` | `conductor` (`PK`), `cant`, `suma`, `promedio`, `actualizado` (`TIME(0)`) | Proyección de reputación calculada por M3. |

---

## 3. Flujo de la Información Paso a Paso

| Paso | Componente y función clave | Operación ejecutada | Estado observable |
| :--- | :--- | :--- | :--- |
| **1** | `api.py` $\rightarrow$ `registrar_calificacion()` | Inserta en `m2.calificaciones` y `m2.outbox_eventos` (`PENDIENTE`) en un único `with transaccion_bd()`. | Respuesta `202 Accepted` (<50 ms). `m2.outbox_eventos` en `PENDIENTE`; `m3.*` sin cambios. |
| **2** | `relay.py` $\rightarrow$ `retransmitir_eventos_outbox()` | Lee `PENDIENTE`, publica en `cola_calificaciones_m2` con `confirm_delivery()` y actualiza a `PUBLICADO`. | `m2.outbox_eventos` en `PUBLICADO`. Mensaje disponible (`Ready`) en RabbitMQ. |
| **3A** | `worker.py` $\rightarrow$ `procesar_mensaje()` | Decodifica UTF-8/JSON y valida tipos y puntaje (`1..5`). | Si falla: `basic_nack(..., requeue=False)` $\rightarrow$ mensaje aislado en `cola_calificaciones_dlq`. |
| **3B** | `worker.py` $\rightarrow$ `procesar_mensaje()` | Ejecuta `pg_try_advisory_xact_lock(calif_id)` e `INSERT INTO m3.inbox_eventos ... ON CONFLICT DO NOTHING`. | Si es duplicado en vuelo o histórico: `ACK` inmediato. Si es nuevo: procesa 40s (mensaje **`Unacked`** en RabbitMQ). |
| **4** | `worker.py` $\rightarrow$ `procesar_mensaje()` | Actualiza `m3.conductores_reputacion` (*upsert*), ejecuta `COMMIT` conjunto con el Inbox y emite `basic_ack()`. | `m3.inbox_eventos` registra `procesado` (+40s respecto a `m2.calificaciones.hora`), promedio actualizado y mensaje borrado de la cola. |

---

## 4. Matriz de Tolerancia a Fallos

| Falla simulada | Dónde actúa la defensa | Mecanismo en código | Resultado garantizado |
| :--- | :--- | :--- | :--- |
| **Caída de RabbitMQ** | `api.py` + `relay.py` | `m2.outbox_eventos` (`PENDIENTE`) + `try/except` en Relay | API sigue respondiendo `202`; Relay publica al volver el broker. Cero pérdidas. |
| **Caída de PostgreSQL** | `relay.py` + `worker.py` | `try/except` global en Relay + `sleep(3)` y `requeue=True` en Worker | Ningún proceso crashea; los mensajes esperan a salvo en RabbitMQ. |
| **Muerte abrupta de Worker (`SIGKILL`)** | `worker.py` + RabbitMQ + PG | `auto_ack=False` + `ROLLBACK` automático por corte TCP | Mensaje pasa de `Unacked` a `Ready` (`redelivered=True`); otro worker lo completa. |
| **Duplicado concurrente (en vuelo)** | `worker.py` (Filtro 1) | `SELECT pg_try_advisory_xact_lock(calif_id)` | Segundo worker descarta en <1 ms con `ACK` sin quedar bloqueado. |
| **Duplicado histórico** | `worker.py` (Filtro 2) | `INSERT INTO m3.inbox_eventos ... ON CONFLICT DO NOTHING` | Descarte inmediato con `ACK`; `cant` y `promedio` intactos. |
| **Mensaje corrupto (*Poison Pill*)** | `comun.py` + `worker.py` | `basic_nack(..., requeue=False)` + `x-dead-letter-routing-key` | Mensaje derivado a `cola_calificaciones_dlq`; cola principal operando normal. |

---

## 5. Puesta en Marcha y Comandos Esenciales

### Requisitos
* Docker Desktop (con Docker Compose v2) en ejecución.
* Windows PowerShell 5.1+ (para el panel `demo.ps1`).

### 1. Arranque limpio con 2 Workers en paralelo
Desde la carpeta raíz del proyecto:
```powershell
docker compose down -v
docker compose up -d --build --scale worker=2
```

### 2. Terminales recomendadas y URLs de monitoreo
* **Terminal 1 — Trazabilidad en tiempo real (logs unificados):**
  ```powershell
  docker compose logs -f api relay worker
  ```
* **Terminal 2 — Control remoto interactivo (`demo.ps1`):**
  ```powershell
  .\demo.ps1
  ```
  *(Si Windows restringe scripts locales, ejecutar sin cambiar la política del sistema:)*
  ```powershell
  powershell -ExecutionPolicy Bypass -File .\demo.ps1
  ```
* **Swagger UI (API M2):** `http://localhost:8000/docs`
* **RabbitMQ Management UI:** `http://localhost:15672/#/queues` *(ver `cola_calificaciones_m2` y `cola_calificaciones_dlq`)*

### 3. Consultas directas a PostgreSQL desde Docker (opcional, equivalentes a la tecla `[T]`)
Ver las 4 tablas de ambos esquemas en una sola ejecución:
```powershell
docker compose exec db psql -U usuario_tp -d bd_pedidos -c "SELECT * FROM m2.calificaciones ORDER BY id; SELECT * FROM m2.outbox_eventos ORDER BY id; SELECT * FROM m3.inbox_eventos ORDER BY calif_id; SELECT * FROM m3.conductores_reputacion ORDER BY conductor;"
```

Consola interactiva `psql` (opcional):
```powershell
docker compose exec db psql -U usuario_tp -d bd_pedidos
```

---

## 6. Control Remoto de Demostración (`demo.ps1`)

El script permite controlar la infraestructura Docker y disparar los endpoints HTTP sin modificar el código Python.

### Referencia de teclas

| Tecla | Acción | Uso didáctico |
| :---: | :--- | :--- |
| **`[1]`** | `POST /calificaciones` (pide puntaje 1–5) | Flujo seguro con Transactional Outbox. |
| **`[2]`** | `POST /calificaciones/sin-outbox` | Demostración del fallo *Dual-Write*. |
| **`[3]`** | `POST /calificaciones/{id}/forzar-duplicado` | Prueba de idempotencia histórica y concurrencia. |
| **`[4]`** | `POST /calificaciones/enviar-corrupto` | Inyección de *Poison Pill* hacia la DLQ. |
| **`[T]`** | `SELECT *` sobre las 4 tablas (`m2` y `m3`) | Inspección instantánea de consistencia eventual. |
| **`[L]`** | `TRUNCATE ... RESTART IDENTITY` + `purge_queue` | Limpia tablas (reinicia IDs a `1`) y vacía colas en 1s. |
| **`[R]`** | `stop` / `start` del contenedor `relay` | Congela eventos en `PENDIENTE` dentro del Outbox. |
| **`[W]`** | `stop` / `start` de **todos** los `worker` | Congela mensajes en estado `Ready` en RabbitMQ. |
| **`[P]`** | `docker pause` / `unpause` solo de `worker-1` | Congela en RAM el estado `Unacked` y el candado PG sin cortar TCP. |
| **`[K]`** | `docker kill` (`SIGKILL`) solo de `worker-1` | Simula caída abrupta para demostrar reentrega sin pérdida. |
| **`[B]`** | `stop` / `start` de `broker` (RabbitMQ) | Simula caída del broker de mensajería. |
| **`[D]`** | `stop` / `start` de `db` (PostgreSQL) | Simula caída temporal de la base de datos. |
| **`[A]`** | Despausa todo y ejecuta `docker compose start` | Restaura todo el entorno a `RUNNING`. |
| **`[X]`** | `down -v` + `up -d --build --scale worker=2` | Reinicio total borrando volúmenes y reconstruyendo imágenes. |
| **`[F]`** / **`[Q]`** | Refrescar estado / Salir | Actualiza el semáforo de contenedores o cierra el menú. |

### Diferencia técnica entre `stop`, `pause` y `kill`
* **Apagar (`stop` — `R`, `W`, `B`, `D`):** Envía `SIGTERM`, cierra el proceso y sus sockets TCP (RabbitMQ revierte `Unacked` $\rightarrow$ `Ready`; PostgreSQL hace `ROLLBACK`).
* **Matar (`kill` — `K`):** Envía `SIGKILL` instantáneo sin ejecutar `finally`, simulando un corte eléctrico o fallo fatal de proceso.
* **Pausar (`pause` — `P`):** Congela la CPU del contenedor vía *cgroups* manteniendo memoria y sockets abiertos: el `sleep(40)`, `Unacked` y `pg_try_advisory_xact_lock` permanecen congelados hasta ejecutar `unpause` (dentro de la ventana `heartbeat=300` de 5 min).

---

## 7. Guion de Demostraciones Prácticas

Antes de cada escenario, asegurarse de tener los servicios activos (`[A]`) y limpiar los datos previos con **`[L]`**.

### Escenario 1: Flujo normal paso a paso (`PENDIENTE` $\rightarrow$ `Ready` $\rightarrow$ `Unacked` $\rightarrow$ `COMMIT`)
1. Presionar **`[R]`** (apaga Relay) y **`[W]`** (apaga Workers).
2. Presionar **`[1]`** + Enter (puntaje `5`) $\rightarrow$ respuesta inmediata `202 Accepted`.
3. Presionar **`[T]`** $\rightarrow$ mostrar `m2.calificaciones` con `id = 1` y `m2.outbox_eventos` congelado en **`PENDIENTE`** (`m3.*` vacío).
4. Presionar **`[R]`** (enciende Relay) $\rightarrow$ en 1s el evento pasa a **`PUBLICADO`** y queda en **`Ready = 1`** en RabbitMQ.
5. Presionar **`[W]`** (enciende Workers) $\rightarrow$ un worker toma el mensaje (**`Unacked = 1`** en RabbitMQ). *(Opcional: presionar **`[P]`** si lo tomó `worker-1` para explicarlo sin apuro y **`[P]`** para reanudar).*
6. Al finalizar los 40s, presionar **`[T]`** $\rightarrow$ mostrar `m3.inbox_eventos` (`procesado` 40s después de `m2.calificaciones.hora`) y `m3.conductores_reputacion` (`cant = 1`, `promedio = 5.00`).

### Escenario 2: Caída de RabbitMQ — Sin Outbox (*Dual-Write*) vs. Con Outbox
1. Presionar **`[B]`** $\rightarrow$ apaga RabbitMQ.
2. Presionar **`[2]`** (`sin-outbox`) $\rightarrow$ error HTTP `500`. Presionar **`[T]`**: la calificación `id = 1` quedó grabada en `m2.calificaciones` pero **no existe en `m2.outbox_eventos`** (huérfana permanente).
3. Con RabbitMQ aún apagado, presionar **`[1]`** + Enter (`con-outbox`) $\rightarrow$ responde **`202 Accepted`**. Presionar **`[T]`**: `calif_id = 2` figura a salvo en `m2.outbox_eventos` como **`PENDIENTE`**. En logs, `relay` reporta alerta cada 3s sin crashear.
4. Presionar **`[B]`** $\rightarrow$ enciende RabbitMQ. El Relay reconecta automáticamente, despacha `id = 2` y el Worker actualiza M3. Al consultar **`[T]`**, `m3.inbox_eventos` contiene únicamente `calif_id = 2`, evidenciando la pérdida irreversible del enfoque sin Outbox (`id = 1`).

### Escenario 3: Caída temporal de PostgreSQL (`db`)
1. Presionar **`[D]`** $\rightarrow$ apaga PostgreSQL.
2. Observar en logs cómo `relay` captura la excepción en su `try/except` global y reintenta cada 3s **sin morir**. Si un worker estaba procesando, espera 3s y emite `basic_nack(..., requeue=True)` devolviendo el mensaje a RabbitMQ.
3. Presionar **`[D]`** $\rightarrow$ revive PostgreSQL; el sistema retoma el procesamiento automáticamente.

### Escenario 4: 1 solo Worker (`prefetch_count=1` + Crash con recuperación)
1. Escalar a 1 worker desde una terminal auxiliar y presionar **`[F]`** en `demo.ps1`:
   ```powershell
   docker compose up -d --scale worker=1
   ```
2. Presionar **`[L]`** para iniciar limpio. Enviar dos calificaciones seguidas: **`[1]`** (puntaje `5` $\rightarrow$ `id = 1`) y **`[1]`** (puntaje `3` $\rightarrow$ `id = 2`).
3. En RabbitMQ verificar **`Unacked = 1`** (`id = 1`) y **`Ready = 1`** (`id = 2`) gracias a `prefetch_count=1`.
4. Presionar **`[K]`** (`SIGKILL` sobre `worker-1`) $\rightarrow$ en RabbitMQ `Unacked` pasa a `0` y `Ready` sube a `2` (`id = 1` no se perdió).
5. Presionar **`[W]`** (o **`[A]`**) para levantar `worker-1` $\rightarrow$ procesa `id = 1` con marca **`[REENTREGADO tras caída previa sin ACK]`** y luego `id = 2`. Al consultar **`[T]`**: `cant = 2`, `suma = 8`, `promedio = 4.00`.
6. Volver a 2 workers antes del siguiente escenario y refrescar con **`[F]`**:
   ```powershell
   docker compose up -d --scale worker=2
   ```

### Escenario 5: Concurrencia con 2 Workers e Idempotencia (Doble Filtro)
1. Presionar **`[L]`**. Enviar `id = 1` con **`[1]`** (puntaje `5`).
2. **Filtro 1 (en vuelo — `pg_try_advisory_xact_lock`):** Mientras el primer worker está en los 40s (se puede pausar `worker-1` con **`[P]`** si fue quien la tomó), presionar **`[3]`** + Enter para duplicar `id = 1`. El segundo worker imprime `[WORKER M3 CONCURRENCIA]` y descarta la copia con `ACK` en <1 ms sin pausarse.
3. Inmediatamente enviar `id = 2` con **`[1]`** (puntaje `3`): comprobar que el segundo worker toma `id = 2` en paralelo sin esperar a `id = 1`. Reanudar **`[P]`** si se había pausado.
4. **Filtro 2 (histórico — `m3.inbox_eventos`):** Una vez finalizados los 40s, volver a presionar **`[3]`** + Enter para `id = 1`. El worker responde al instante `[WORKER M3 IDEMPOTENCIA]` y envía `ACK`. Con **`[T]`**, confirmar que `m3.conductores_reputacion` mantiene `cant = 2`, `suma = 8`, `promedio = 4.00`.

### Escenario 6: Mensaje Corrupto (*Poison Pill*) y Dead Letter Queue (DLQ)
1. Presionar **`[4]`** (`POST /calificaciones/enviar-corrupto`) $\rightarrow$ inyecta `{calificacion_id: SIN_COMILLAS_Y_SIN_CERRAR` en `cola_calificaciones_m2`.
2. Verificar en logs: `[POISON PILL -> DLQ | WORKER M3] Mensaje corrupto descartado... Desviando a 'cola_calificaciones_dlq'`. El worker **no crashea ni entra en bucle**.
3. Verificar en RabbitMQ (`http://localhost:15672/#/queues`) o en `GET /resumen` que `cola_calificaciones_dlq` registra **1 mensaje** en cuarentena mientras `cola_calificaciones_m2` queda vacía y lista para recibir nuevas calificaciones válidas.

---

## 8. Trade-offs y Evolución hacia Producción

Esta implementación prioriza la claridad arquitectónica y la observabilidad en tiempo real. En un entorno productivo de alto volumen se aplican tres evoluciones directas sobre esta misma base:

* **Polling Publisher $\rightarrow$ Change Data Capture (CDC):** Reemplazar el sondeo cada 1 segundo de `relay.py` por un conector CDC (ej. **Debezium**) que lea el *Write-Ahead Log (WAL)* de PostgreSQL con latencia de milisegundos y sin carga de consultas `SELECT`.
* **Transacción larga (`sleep(40)`) $\rightarrow$ Patrón *Lease* con TTL:** El `sleep(40)` dentro de la transacción SQL permite observar `Unacked` y el `ROLLBACK` automático ante `SIGKILL`. En producción, para tareas que demoren minutos o invoquen servicios externos, se evita sostener conexiones SQL abiertas registrando un estado `'PROCESANDO'` con marca de expiración (`expira_en`) renovable (*lease*), permitiendo que otro worker reclame la tarea si el primero muere y vence el TTL.
* **Esquemas lógicos $\rightarrow$ Bases de datos físicas independientes:** Al no existir claves foráneas ni consultas cruzadas entre `m2.*` y `m3.*`, migrar a dos servidores PostgreSQL físicos independientes solo requiere modificar la cadena de conexión de `worker.py`, sin cambiar ninguna consulta SQL.

---

## 9. Referencias y Bibliografía Técnica

* **Microsoft Azure Architecture Center — Asynchronous Request-Reply Pattern:**  
  https://learn.microsoft.com/es-es/azure/architecture/patterns/asynchronous-request-reply
* **Microsoft Azure Architecture Center — Queue-Based Load Leveling Pattern:**  
  https://learn.microsoft.com/es-es/azure/architecture/patterns/queue-based-load-leveling
* **RabbitMQ Official Documentation — Work Queues (Python / Pika, `prefetch_count`, `auto_ack=False` y durabilidad):**  
  https://www.rabbitmq.com/tutorials/tutorial-two-python
* **Microservices.io (Chris Richardson) — Pattern: Transactional Outbox:**  
  https://microservices.io/patterns/data/transactional-outbox.html
* **Microservices.io (Chris Richardson) — Pattern: Polling Publisher:**  
  https://microservices.io/patterns/data/polling-publisher.html
* **AWS Prescriptive Guidance — Transactional Outbox Pattern:**  
  https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html