# Módulo M2 (Clientes) - Calificación de Conductores con Transactional Outbox y Colas Asíncronas

**Desarrollo de Software · UTN FRRe**  
Trabajo Práctico Integrador (TPI) · Plataforma de Movilidad Urbana.  
**Tema:** Colas de mensajes y procesamiento asíncrono (Tema 2) y Patrón Transactional Outbox (Tema 27).

---

## 1. Investigación Conceptual

### 1.1. Colas de Mensajes y Procesamiento Asíncrono

**Definición y Problema que Resuelve**  
En una arquitectura web tradicional basada en comunicación síncrona, cuando un cliente envía una solicitud HTTP a una API, la conexión de red permanece abierta y bloqueada hasta que el servidor ejecuta toda la lógica de negocio y devuelve la respuesta. Si la operación incluye tareas pesadas o que involucran a otros módulos (como recálculo de reputación, auditoría o notificaciones), la demora se traslada directamente al usuario. Este acoplamiento síncrono provoca agotamiento de hilos, saturación de memoria y errores de tiempo de espera agotado (Gateway Timeout).

El uso de colas de mensajes y procesamiento asíncrono resuelve este cuello de botella intercalando un gestor de mensajería (**Message Broker**, como RabbitMQ) entre el servidor web (**Productor**) y el programa encargado de ejecutar la tarea pesada (**Consumidor** o **Worker**). Una cola es una estructura temporal de tipo FIFO (First In, First Out) donde cada tarea viaja encapsulada como un paquete de texto estructurado en formato JSON.

**Desacoplamiento HTTP y Comportamiento ante Fallas del Consumidor**  
El flujo técnico opera en dos fases independientes:
* **Fase 1: Respuesta HTTP inmediata (Código 202 Accepted):** Cuando la API recibe la petición, valida la estructura de los datos de entrada y registra la tarea sin ejecutar el procesamiento pesado. El mensaje ingresa a la cola del broker y la API responde en milisegundos con el código HTTP **202 Accepted**.
* **Fase 2: Retención y recuperación ante caída del Worker:** El broker almacena cada mensaje en disco duro y gestiona su ciclo de vida mediante confirmaciones manuales (`ACK` / `NACK`). Si el contenedor del worker se apaga o falla a mitad de una tarea antes de emitir el `ACK`, el broker detecta la desconexión TCP, saca el mensaje del estado `Unacked` y lo devuelve automáticamente al estado `Ready` al principio de la fila para que no se pierda.

**Características Principales**
* **Desacoplamiento temporal:** El productor y el consumidor no necesitan estar encendidos en el mismo instante.
* **Durabilidad y persistencia:** Tanto la cola (`durable=True`) como los mensajes (`delivery_mode=2`) se graban en disco duro.
* **Control de flujo (Prefetch):** Mediante `prefetch_count=1`, el broker entrega un único mensaje a la vez a cada worker, protegiendo su memoria RAM.

---

### 1.2. Patrón Transactional Outbox

**Definición y Problema que Resuelve**  
En los sistemas distribuidos, un servicio suele necesitar realizar dos acciones conjuntas: modificar el estado en su **Base de Datos** e informar ese suceso publicando un evento en el **Broker de Mensajes**. Dado que la base de datos (PostgreSQL) y el broker (RabbitMQ) son servidores independientes, no comparten una transacción atómica única.

Intentar ejecutar ambas escrituras de forma secuencial dentro del mismo endpoint genera el **Problema de la Doble Escritura (Dual-Write Problem)**:
1. Si se hace `COMMIT` en la base de datos y falla la publicación en RabbitMQ, el registro queda guardado pero el evento se pierde permanentemente.
2. Si se publica primero en el broker y falla la base de datos (`ROLLBACK`), el worker procesará un evento fantasma sobre un dato que no existe.

**Garantía de Consistencia mediante Transacción Local y Proceso Posterior**  
El patrón **Transactional Outbox** soluciona esto dividiendo la operación en dos mecanismos:
* **Mecanismo 1: Escritura atómica en tabla Outbox local:** En el mismo motor de base de datos se crea una tabla adicional llamada `outbox_eventos`. La API abre una única transacción local (`BEGIN`), inserta el registro principal y el evento JSON con estado `PENDIENTE` en la tabla outbox, ejecutando un único `COMMIT` atómico.
* **Mecanismo 2: Retransmisión asíncrona (Message Relay):** Un proceso independiente en segundo plano consulta periódicamente la tabla `outbox_eventos` (`SELECT ... WHERE estado = 'PENDIENTE'`) y publica los mensajes en RabbitMQ. Solo cuando el broker confirma la recepción, el proceso actualiza la fila a estado `PUBLICADO`.

**Idempotencia en el Consumidor**  
El patrón garantiza semántica de entrega **al menos una vez (At-least-once)**: ante una falla del relay justo antes de marcar como `PUBLICADO`, el evento puede enviarse duplicado. Por ello, es obligatorio que el worker consumidor sea **idempotente**, verificando en la base de datos si el identificador ya fue procesado antes de volver a ejecutar la lógica de negocio.

---

## 2. Relación con el Trabajo Práctico Integrador (Plataforma de Movilidad Urbana)

En el marco del Trabajo Práctico Integrador, este repositorio implementa el **Módulo M2: Clientes**, abarcando específicamente el requerimiento **RF-2.5 (Calificación del conductor)**:
* *"El cliente deberá poder registrar una valoración posterior a un viaje completado, evitando duplicados para el mismo viaje"*.

### 2.1. Escenario en la Plataforma
Cuando un cliente califica a un conductor desde su aplicación móvil tras finalizar un viaje, M2 registra la valoración y debe notificar asíncronamente al **Módulo M3 (Conductores y Vehículos)** para actualizar su reputación histórica y evaluar su habilitación (`RF-3.1`, `RF-3.4`), y a **M8 (Notificaciones y Soporte)** para auditar la reseña (`RF-8.8`).

Aplicar **Transactional Outbox** y **Colas Asíncronas** garantiza que:
1. La aplicación del cliente responda de inmediato (`202 Accepted`) sin demoras.
2. Si RabbitMQ sufre una caída en hora pico, la calificación del cliente no se pierda y quede resguardada de forma segura en la tabla outbox de PostgreSQL.
3. Si un mensaje se reenvía por duplicado, el consumidor (Worker de M3) lo detecte mediante control de idempotencia (tabla `eventos_procesados`) y evite sumar dos veces la misma puntuación a la reputación del conductor.

---

## 3. Arquitectura de la Solución (Contenedores Docker)

El sistema se compone de 5 servicios integrados mediante Docker Compose:
1. **`db` (PostgreSQL 16):** Para simplificar, M2 y M3 comparten el servidor, pero **cada módulo usa solo sus tablas**:
   * **M2 (Clientes):** `calificaciones` y la bandeja de salida `outbox_eventos`.
   * **M3 (Conductores):** `reputacion_conductor` (cantidad y suma de puntajes de cada conductor) y `eventos_procesados` (eventos ya sumados, para la idempotencia).
2. **`broker` (RabbitMQ 3):** Gestiona la cola durable `cola_calificaciones_m2`.
3. **`api` (FastAPI / M2):** Expone los endpoints HTTP y el panel de la demo en el puerto `8000`.
4. **`relay` (M2):** Lee los eventos `PENDIENTE` del outbox, los publica en RabbitMQ y los marca `PUBLICADO`.
5. **`worker` (M3):** Consume la cola con confirmación manual (`ACK`) y suma cada calificación a la reputación del conductor **una sola vez**.

**M2 y M3 se comunican únicamente por mensajes en RabbitMQ.** El worker de M3 nunca lee ni escribe las tablas de M2.

### 3.1. Consumidor idempotente

Cada mensaje lleva un `evento_id`. El worker, en **una sola transacción**:
1. Ejecuta `INSERT INTO eventos_procesados (evento_id) ... ON CONFLICT DO NOTHING`. La clave primaria no admite repetidos: si el evento ya estaba, no se inserta nada.
2. Solo si se insertó, suma el puntaje en `reputacion_conductor`.

Después del `COMMIT` envía el `ACK`. Si un evento llega dos veces (el relay lo republicó o RabbitMQ lo reentregó), la segunda vez no se suma: el promedio del conductor no cambia. Como el control y el registro son una sola operación en la base, funciona aunque haya varios workers a la vez.

---

## 4. Guía de Ejecución y Demostración Práctica

Asegúrate de tener **Docker Desktop** abierto y ejecutándose en tu PC. Abre tu terminal (PowerShell o CMD) en la carpeta raíz del proyecto y ejecuta:

```powershell
docker compose up -d --build
```

- **Panel de la demo (para proyectar):** [http://localhost:8000/panel](http://localhost:8000/panel)
- **Panel interactivo Swagger UI (API):** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Consola web de RabbitMQ:** [http://localhost:15672](http://localhost:15672) *(Usuario: `guest`, Clave: `guest`)*
- **Ver logs en tiempo real (con correlación de IDs y pasos):**
  ```powershell
  docker compose logs -f api relay worker
  ```

### 4.1. Panel y modo paso a paso

El panel muestra en vivo las tres partes del sistema: **M2** (calificaciones guardadas y outbox), **RabbitMQ** (mensajes Ready y Unacked) y **M3** (calificaciones contadas y reputación). Arriba compara *guardadas en M2* con *contadas en M3*: tienen que coincidir. Si hay más guardadas, se perdió un evento; si hay más contadas, se contó un duplicado.

Con **Modo paso a paso: ACTIVO**, el relay y el worker se detienen en 4 momentos y esperan a que se presione **Avanzar** (botón o **barra espaciadora**):

| Pausa | Servicio | Momento | Qué se explica |
|---|---|---|---|
| 1 | relay | El evento está `PENDIENTE` en el outbox y todavía no se publicó | El evento está a salvo en la base. Si RabbitMQ se cae, no se pierde. |
| 2 | relay | El evento ya está en RabbitMQ pero sigue `PENDIENTE` | Si el relay se cae acá, lo vuelve a publicar: entrega *al menos una vez*. |
| 3 | worker | El worker recibió el mensaje (`Unacked`) y no lo procesó | Momento para `docker compose kill worker`: el mensaje vuelve a `Ready`. |
| 4 | worker | El worker sumó y guardó (`COMMIT`) pero no envió el `ACK` | Si se cae acá, RabbitMQ lo reentrega y la idempotencia evita sumarlo dos veces. |

Las pausas se pueden manejar también desde Swagger: `POST /demo/paso-a-paso?activo=true` y `POST /demo/avanzar`. Con el modo apagado, todo corre solo.

El botón **Empezar de cero** (dos clics para confirmar, o `POST /demo/reiniciar`) borra todas las calificaciones, el outbox y la reputación, vacía la cola y apaga el modo paso a paso, sin reiniciar los contenedores. Si un mensaje viejo estaba en camino, el worker lo descarta.

---

## 5. Guion de Demostración para el Coloquio (5 Actos)

Todos los actos se pueden hacer desde el **panel** (`/panel`). Los botones equivalen a los endpoints de Swagger: **Calificar (con outbox)** = `POST /calificaciones`, **Calificar sin outbox** = `POST /calificaciones/sin-outbox`, **Forzar duplicado** = `POST /calificaciones/{id}/forzar-duplicado`.

### Acto 1: Flujo Normal, paso a paso
1. Activa **Modo paso a paso** y presiona **Calificar (con outbox)**. Responde **`202 Accepted`** al instante.
2. **Pausa 1:** el evento está `PENDIENTE` en el outbox, guardado en la misma transacción que la calificación. Avanzar.
3. **Pausa 2:** el evento ya está en RabbitMQ pero todavía no se marcó `PUBLICADO`. Avanzar.
4. **Pausa 3:** el worker de M3 recibió el mensaje, que está `Unacked`. Avanzar.
5. **Pausa 4:** M3 ya sumó la calificación (`COMMIT`) pero falta el `ACK`. Avanzar.
6. Guardadas y contadas coinciden. Desactiva el modo paso a paso para los actos siguientes (o déjalo para explicar cada uno).

### Acto 2: Caída del Consumidor (Worker) y Fila en Espera
1. Detén el contenedor del worker:
   ```powershell
   docker compose stop worker
   ```
2. Presiona varias veces **Calificar (con outbox)**. La API sigue respondiendo `202 Accepted` y los mensajes se acumulan en **`Ready`**.
3. Vuelve a encender el worker:
   ```powershell
   docker compose start worker
   ```
   Los mensajes pasan de a uno a `Unacked` y se procesan secuencialmente gracias a `prefetch_count=1`.

### Acto 3: Mecanismo ACK y Recuperación ante Caída del Worker
1. Con el modo paso a paso activo, califica y avanza hasta la **Pausa 3** (mensaje `Unacked`).
2. Mata de golpe al worker:
   ```powershell
   docker compose kill worker
   ```
3. En el panel, el mensaje vuelve de `Unacked` a **`Ready`** porque se cortó la conexión sin `ACK`.
4. Inicia el worker (`docker compose start worker`): en los logs aparece `[REENTREGADO]` y la calificación se cuenta una sola vez.
5. *Variante:* matar al worker en la **Pausa 4** (ya sumó, falta el `ACK`). Al volver, RabbitMQ lo reentrega y el worker muestra `IDEMPOTENCIA`: las contadas no cambian.

### Acto 4: Caída del Broker y Patrón Transactional Outbox
1. Apaga RabbitMQ:
   ```powershell
   docker compose stop broker
   ```
2. Presiona **Calificar sin outbox**: devuelve **Error 500**. La calificación quedó guardada pero el evento se perdió (*problema de doble escritura / dual-write*). El panel la muestra como calificación sin evento en el outbox.
3. Presiona **Calificar (con outbox)**: devuelve **`202 Accepted`**, porque el evento queda resguardado en `outbox_eventos` con estado `PENDIENTE`.
4. Enciende RabbitMQ (`docker compose start broker`): en pocos segundos el relay publica el evento pendiente y M3 lo cuenta. Al final queda **una guardada de más que nunca se cuenta**: la de sin outbox.

### Acto 5: Idempotencia del Consumidor
1. Presiona **Forzar duplicado del último publicado**. El evento vuelve a `PENDIENTE` y el relay lo publica otra vez (como si se hubiera caído antes de marcarlo `PUBLICADO`).
2. En los logs, el worker muestra **`IDEMPOTENCIA`**: ese `evento_id` ya estaba en `eventos_procesados`, así que no se suma de nuevo. Las contadas y el promedio del conductor no cambian.

---

## 6. Limpieza del Entorno
Para apagar los contenedores y borrar por completo los volúmenes de la base de datos y la cola:
```powershell
docker compose down -v
```

---

## 7. Referencias Bibliográficas y Documentación Técnica
* **Microservices.io (Chris Richardson):** Pattern *Transactional Outbox* y *Polling Publisher* (`https://microservices.io/patterns/data/transactional-outbox.html`).
* **AWS Prescriptive Guidance:** *Transactional Outbox Pattern* en arquitecturas distribuidas de la nube.
* **Microsoft Learn (Azure Architecture Center):** Patrones *Asynchronous Request-Reply* y *Queue-Based Load Leveling*.
* **RabbitMQ Official Documentation:** *Work Queues*, persistencia durable, confirmaciones manuales y control de flujo (`basic_qos`).