-- En el TPI real cada módulo tiene su propia base de datos. Para que la demo sea simple,
-- M2 y M3 comparten el servidor PostgreSQL, pero cada uno usa SOLO sus tablas.
-- M2 y M3 se comunican únicamente por mensajes en RabbitMQ.

-- =====================================================================
-- M2 (Clientes)
-- =====================================================================
CREATE TABLE calificaciones (
    id SERIAL PRIMARY KEY,
    viaje_id VARCHAR(50) NOT NULL,
    cliente_id VARCHAR(50) NOT NULL,
    conductor_id VARCHAR(50) NOT NULL,
    puntaje INTEGER NOT NULL CHECK (puntaje BETWEEN 1 AND 5),
    comentario TEXT,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE outbox_eventos (
    id SERIAL PRIMARY KEY,
    calificacion_id INTEGER NOT NULL,
    tipo_evento VARCHAR(50) NOT NULL,
    payload TEXT NOT NULL,
    estado VARCHAR(30) NOT NULL,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================================
-- M3 (Conductores)
-- =====================================================================
-- Reputación de cada conductor: el promedio es suma / cantidad.
CREATE TABLE reputacion_conductor (
    conductor_id VARCHAR(50) PRIMARY KEY,
    cantidad INTEGER NOT NULL,
    suma INTEGER NOT NULL
);

-- Eventos que M3 ya sumó. La clave primaria impide registrar dos veces el mismo evento:
-- es lo que hace idempotente al worker.
CREATE TABLE eventos_procesados (
    evento_id VARCHAR(60) PRIMARY KEY,
    fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================================
-- Control del modo paso a paso (solo para la exposición)
-- =====================================================================
CREATE TABLE demo_control (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    paso_a_paso BOOLEAN NOT NULL,
    -- Sube en 1 con cada "Empezar de cero". Los mensajes de antes del reinicio se descartan.
    generacion INTEGER NOT NULL DEFAULT 1
);
INSERT INTO demo_control (id, paso_a_paso) VALUES (1, FALSE);

-- Una fila por servicio que está detenido en una pausa, esperando "Avanzar".
CREATE TABLE demo_pausas (
    servicio VARCHAR(20) PRIMARY KEY,
    numero INTEGER NOT NULL,
    descripcion TEXT NOT NULL,
    calificacion_id INTEGER,
    desde TIMESTAMP NOT NULL DEFAULT now(),
    avanzar BOOLEAN NOT NULL DEFAULT FALSE
);
