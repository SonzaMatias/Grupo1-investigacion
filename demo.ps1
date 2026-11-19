# Control remoto para la demo de Outbox / Inbox / DLQ (sin modificar Python)
# Ejecutar desde la carpeta del proyecto: .\demo.ps1
# Si Windows bloquea scripts locales: powershell -ExecutionPolicy Bypass -File .\demo.ps1

$API_BASE = "http://localhost:8000"

function Obtener-Servicios {
    $lineas = @(docker compose ps -a --format "{{.Service}}|{{.Name}}|{{.State}}" 2>$null)
    $lista = @()
    foreach ($linea in $lineas) {
        if ($linea -match "^([^|]+)\|([^|]+)\|(.+)$") {
            $lista += [PSCustomObject]@{
                Servicio = $matches[1].Trim()
                Nombre   = $matches[2].Trim()
                Estado   = $matches[3].Trim()
            }
        }
    }
    return @($lista | Sort-Object Servicio, Nombre)
}

function Obtener-Worker1($servicios) {
    return (@($servicios | Where-Object { $_.Servicio -eq "worker" } | Sort-Object Nombre) | Select-Object -First 1)
}

function Esperar-Tecla {
    Write-Host "`nPresiona cualquier tecla para volver al menu..." -ForegroundColor DarkGray
    [Console]::ReadKey($true) | Out-Null
}

function Alternar-Servicio($nombreServicio, $servicios) {
    $items = @($servicios | Where-Object { $_.Servicio -eq $nombreServicio })
    if ($items.Count -eq 0) {
        Write-Host "`n[AVISO] No se encontro el servicio '$nombreServicio'. Levanta Docker primero con [X]." -ForegroundColor Yellow
        Start-Sleep -Seconds 2
        return
    }
    $algunoActivo = @($items | Where-Object { $_.Estado -eq "running" -or $_.Estado -eq "paused" }).Count -gt 0
    if ($algunoActivo) {
        Write-Host "`n>>> Deteniendo '$nombreServicio'..." -ForegroundColor Yellow
        foreach ($i in @($items | Where-Object { $_.Estado -eq "paused" })) {
            docker unpause $i.Nombre | Out-Null
        }
        if ($nombreServicio -in @("relay", "worker")) {
            docker compose stop -t 2 $nombreServicio
        } else {
            docker compose stop $nombreServicio
        }
    } else {
        Write-Host "`n>>> Encendiendo '$nombreServicio'..." -ForegroundColor Green
        docker compose start $nombreServicio
    }
}

function Invocar-Api($url, $bodyJson = $null) {
    try {
        if ($null -ne $bodyJson) {
            $resp = Invoke-RestMethod -Uri $url -Method Post -ContentType "application/json; charset=utf-8" -Body $bodyJson -TimeoutSec 10
        } else {
            $resp = Invoke-RestMethod -Uri $url -Method Post -TimeoutSec 10
        }
        Write-Host "`n[OK] Respuesta de la API:" -ForegroundColor Green
        Write-Host ($resp | ConvertTo-Json -Depth 4)
    } catch {
        $detalle = $null
        if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
            $detalle = $_.ErrorDetails.Message
        } elseif ($_.Exception.Response) {
            try {
                $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $detalle = $reader.ReadToEnd()
                $reader.Close()
            } catch {}
        }
        Write-Host "`n[ERROR HTTP] La API devolvio un fallo:" -ForegroundColor Red
        if ($detalle) { Write-Host $detalle -ForegroundColor Yellow }
        else { Write-Host $_.Exception.Message -ForegroundColor Yellow }
    }
}

function Mostrar-Tablas($servicios) {
    $db = @($servicios | Where-Object { $_.Servicio -eq "db" }) | Select-Object -First 1
    if (-not $db -or $db.Estado -ne "running") {
        Write-Host "`n[AVISO] PostgreSQL ('db') no esta corriendo. Encendelo con [D] primero." -ForegroundColor Red
        return
    }
    Write-Host ""
    $sql = @"
\echo === [M2] m2.calificaciones ===
SELECT * FROM m2.calificaciones ORDER BY id;
\echo === [M2] m2.outbox_eventos ===
SELECT * FROM m2.outbox_eventos ORDER BY id;
\echo === [M3] m3.inbox_eventos ===
SELECT * FROM m3.inbox_eventos ORDER BY calif_id;
\echo === [M3] m3.conductores_reputacion ===
SELECT * FROM m3.conductores_reputacion ORDER BY conductor;
"@
    $sql.Replace("`r", "") | docker compose exec -T db psql -U usuario_tp -d bd_pedidos
}

function Limpiar-Datos($servicios) {
    Write-Host "`n>>> Limpiando tablas de PostgreSQL y colas de RabbitMQ..." -ForegroundColor Yellow

    $db = @($servicios | Where-Object { $_.Servicio -eq "db" }) | Select-Object -First 1
    if ($db -and $db.Estado -eq "running") {
        $sql = @"
SET lock_timeout = '3s';
TRUNCATE TABLE m2.outbox_eventos, m2.calificaciones, m3.inbox_eventos, m3.conductores_reputacion RESTART IDENTITY CASCADE;
"@
        $sql.Replace("`r", "") | docker compose exec -T db psql -U usuario_tp -d bd_pedidos
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[AVISO] No se pudo truncar (probable worker procesando o pausado). Detene workers con [W] y reintenta." -ForegroundColor Red
        } else {
            Write-Host "[OK] Tablas vaciadas y secuencias reiniciadas a ID = 1." -ForegroundColor Green
        }
    } else {
        Write-Host "[OMITIDO] PostgreSQL ('db') esta apagado." -ForegroundColor DarkYellow
    }

    $broker = @($servicios | Where-Object { $_.Servicio -eq "broker" }) | Select-Object -First 1
    if ($broker -and $broker.Estado -eq "running") {
        docker compose exec -T broker rabbitmqctl purge_queue cola_calificaciones_m2 2>$null | Out-Null
        docker compose exec -T broker rabbitmqctl purge_queue cola_calificaciones_dlq 2>$null | Out-Null
        Write-Host "[OK] Colas 'cola_calificaciones_m2' y 'cola_calificaciones_dlq' purgadas." -ForegroundColor Green
    } else {
        Write-Host "[OMITIDO] RabbitMQ ('broker') esta apagado." -ForegroundColor DarkYellow
    }
}

function Dibujar-Menu($servicios) {
    Clear-Host
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "   CONTROL REMOTO DE DEMO - OUTBOX / INBOX / DLQ (M2 -> M3) " -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan

    if (@($servicios).Count -eq 0) {
        Write-Host "  (No hay contenedores creados. Presiona [X] para iniciar)" -ForegroundColor Yellow
    } else {
        foreach ($s in @($servicios)) {
            $color = switch ($s.Estado) {
                "running" { "Green" }
                "paused"  { "Yellow" }
                default   { "Red" }
            }
            $etiqueta = "{0,-26}" -f $s.Nombre
            Write-Host "  $etiqueta : " -NoNewline
            Write-Host $s.Estado.ToUpper() -ForegroundColor $color
        }
    }

    Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
    Write-Host "  ACCIONES HTTP (API M2):" -ForegroundColor White
    Write-Host "  [1] POST Calificacion CON Outbox      (/calificaciones)"
    Write-Host "  [2] POST Calificacion SIN Outbox      (/calificaciones/sin-outbox)"
    Write-Host "  [3] POST Forzar duplicado de ID       (/forzar-duplicado)"
    Write-Host "  [4] POST Inyectar JSON corrupto       (Poison Pill -> DLQ)"
    Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
    Write-Host "  DATOS E INFRAESTRUCTURA:" -ForegroundColor White
    Write-Host "  [T] Ver las 4 tablas de PostgreSQL    (m2 y m3)"
    Write-Host "  [L] Limpiar tablas (ID=1) y purgar colas RabbitMQ"
    Write-Host "  [R] Apagar / Encender RELAY           (congela PENDIENTE en Outbox)"
    Write-Host "  [W] Apagar / Encender WORKERS (todos) (congela Ready en RabbitMQ)"
    Write-Host "  [P] Pausar / Reanudar WORKER 1        (congela Unacked + candado)"
    Write-Host "  [K] MATAR WORKER 1 de golpe (SIGKILL) (simula crash -> redelivery)"
    Write-Host "  [B] Apagar / Encender BROKER          (prueba Dual-Write vs Outbox)"
    Write-Host "  [D] Apagar / Encender BD              (prueba resiliencia ante DB)"
    Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
    Write-Host "  [A] Restaurar TODO a RUNNING  |  [X] Reinicio limpio (down -v)"
    Write-Host "  [F] Refrescar estado          |  [Q] Salir"
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "Presiona una tecla: " -NoNewline
}

while ($true) {
    $servicios = @(Obtener-Servicios)
    Dibujar-Menu $servicios

    $tecla = [Console]::ReadKey($true).KeyChar.ToString().ToUpper()
    Write-Host $tecla

    switch ($tecla) {
        "1" {
            $entrada = Read-Host "`nPuntaje (1-5, Enter para 5)"
            $puntaje = if ($entrada -match "^[1-5]$") { [int]$entrada } else { 5 }
            $json = @{ viaje = "v_101"; cliente = "cli_1"; conductor = "cond_1"; puntaje = $puntaje } | ConvertTo-Json -Compress
            Invocar-Api "$API_BASE/calificaciones" $json
            Esperar-Tecla
        }
        "2" {
            $json = @{ viaje = "v_101"; cliente = "cli_1"; conductor = "cond_1"; puntaje = 5 } | ConvertTo-Json -Compress
            Invocar-Api "$API_BASE/calificaciones/sin-outbox" $json
            Esperar-Tecla
        }
        "3" {
            $idTexto = Read-Host "`nID de calificacion a duplicar (Enter para 1)"
            $califId = if ($idTexto -match "^\d+$") { [int]$idTexto } else { 1 }
            Invocar-Api "$API_BASE/calificaciones/$califId/forzar-duplicado"
            Esperar-Tecla
        }
        "4" {
            Invocar-Api "$API_BASE/calificaciones/enviar-corrupto"
            Esperar-Tecla
        }
        "T" {
            Mostrar-Tablas $servicios
            Esperar-Tecla
        }
        "L" {
            Limpiar-Datos $servicios
            Esperar-Tecla
        }
        "R" { Alternar-Servicio "relay" $servicios }
        "W" { Alternar-Servicio "worker" $servicios }
        "B" { Alternar-Servicio "broker" $servicios }
        "D" { Alternar-Servicio "db" $servicios }
        "P" {
            $w1 = Obtener-Worker1 $servicios
            if (-not $w1) {
                Write-Host "`n[AVISO] No se encontro worker-1." -ForegroundColor Yellow
                Start-Sleep -Seconds 2
            } elseif ($w1.Estado -eq "running") {
                Write-Host "`n>>> Pausando $($w1.Nombre) (mantiene Unacked y candado PG)..." -ForegroundColor Yellow
                docker pause $w1.Nombre
            } elseif ($w1.Estado -eq "paused") {
                Write-Host "`n>>> Reanudando $($w1.Nombre)..." -ForegroundColor Green
                docker unpause $w1.Nombre
            } else {
                Write-Host "`n[AVISO] $($w1.Nombre) esta detenido ($($w1.Estado)). Encendelo con [W] o [A]." -ForegroundColor Red
                Start-Sleep -Seconds 2
            }
        }
        "K" {
            $w1 = Obtener-Worker1 $servicios
            if ($w1 -and ($w1.Estado -eq "running" -or $w1.Estado -eq "paused")) {
                Write-Host "`n>>> Matando abruptamente $($w1.Nombre) (SIGKILL)..." -ForegroundColor Red
                docker kill $w1.Nombre
            } else {
                Write-Host "`n[AVISO] worker-1 no esta activo para matar." -ForegroundColor Yellow
                Start-Sleep -Seconds 1
            }
        }
        "A" {
            Write-Host "`n>>> Restaurando todos los servicios..." -ForegroundColor Green
            foreach ($s in @($servicios | Where-Object { $_.Estado -eq "paused" })) {
                docker unpause $s.Nombre | Out-Null
            }
            docker compose start
        }
        "X" {
            Write-Host "`nSeguro que queres borrar los volumenes y reiniciar de cero? (S/N): " -NoNewline -ForegroundColor Red
            $conf = [Console]::ReadKey($true).KeyChar.ToString().ToUpper()
            Write-Host $conf
            if ($conf -eq "S") {
                docker compose down -v
                docker compose up -d --build --scale worker=2
            }
        }
        "F" { continue }
        "Q" { break }
    }
}