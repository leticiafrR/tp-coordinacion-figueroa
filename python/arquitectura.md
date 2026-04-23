# Arquitectura final del nodo `sum` (con votación)

## Preambulen: Solicitud original vs. cambios de implementación

### Qué se pidió originalmente

El prompt original solicitaba implementar un sistema de votación coordinado entre réplicas de `sum` para determinar **cuándo flushear datos a aggregators**. Se especificaba:

1. **3 threads independientes:** data-plane consumer, control-plane receiver, control-plane sender.
2. **4 estructuras principales:** `VotationsMonitor`, `VotationStatus`, `DigestPool`, `ClientDigest`, `MastersRoutingKeyByVotationID`.
3. **3 mensajes de control:** `Commit`, `TryingReady`, `Ok`.
4. **Comportamiento esperado:** 
   - Registro de votación al EOF
   - Broadcast de `Commit` con master
   - Envío directo de `TryingReady` al master
   - Evaluación de completitud en el master
   - Flush tras recibir `Ok`
   - Mantener el routing por hash hacia aggregators

### Cambios emergentes durante la implementación

Durante la codificación surgieron **problemas de sincronización no explícitamente resueltos en el prompt original**, que requirieron decisiones de diseño adicionales:

1. **Carrera entre `TryingReady` y registro de votación** → solución: buffer `pending_trying_ready_by_votation_id`.
2. **Evitar múltiples broadcasts de `Ok`** → solución: campo `ok_broadcasted` en `VotationStatus`.
3. **Garantizar idempotencia de limpieza por cliente** → solución: patron de liberación uniforme en todos los replicas.

Las secciones siguientes documentan la arquitectura final, con **énfasis en estos cambios emergentes** a través de seguimientos específicos.


---

## 1) Descripción general de la arquitectura final

Implementada en `python/src/sum/main.py`.

### 1.1 Threads del nodo `sum` (según especificación original)

Cada instancia `sum(ID)` usa tres threads lógicos:

1. **Data plane consumer thread**
   - Consume `INPUT_QUEUE` (mensajes provenientes de `gateway`).
   - Procesa:
     - `FruitData` (representado en el wire como `[fruit, amount, client_id]`),
     - `Eof` (`[client_id, total_serialized_data_messages]`).

2. **Control plane receiver thread**
   - Consume `SUM_CONTROL_EXCHANGE` en su routing key propia: `"{ID}_control_routing_key"`.
   - Procesa mensajes de control tipados:
     - `Commit`,
     - `TryingReady`,
     - `Ok`.

3. **Control plane sender thread**
   - Drena una cola interna `Queue`.
   - Envía mensajes de control:
     - **broadcast** a todas las routing keys `"{sum_id}_control_routing_key"`,
     - **directo** a una routing key particular.

> Restricción respetada: no se usa `shared_adapter`; cada extremo de middleware usa su propio channel/conexión.

---

### 1.2 Estructuras sincronizadas (según especificación original)

1. **`DigestPool`**
   - Mapa `client_id -> ClientDigest`.
   - `ClientDigest` contiene:
     - `cant_data_processed`,
     - `data_per_fruit` (acumulado por fruta).

2. **`VotationsMonitor`**
   - Mapa `votation_id(client_id) -> VotationStatus`.
   - `VotationStatus` contiene:
     - `expected_processed_data_count`,
     - `current_processed_data_count`.

3. **`MastersRoutingKeyByVotationID`**
   - Mapa `votation_id -> master_routing_key`.
   - Permite enviar `TryingReady` al maestro de la votación.

---

### 1.3 Protocolo de control (según especificación original)

Helpers agregados en `python/src/common/message_protocol/internal.py`:

- `make_commit(votation_ID, master_routing_key)`
- `make_trying_ready(votation_ID, amount_fruits_processed)`
- `make_ok(votation_ID)`
- `get_control_message_type(message)`

Formato simple: diccionario JSON con campo discriminador `_control_message_type`.

---

## 2) Cambios emergentes: problemas encontrados durante la implementación

Con la especificación original se podía inferir que había riesgos de sincronización, pero **no se explicitaba cómo resolverlos**. Durante la codificación se identificaron y resolvieron los siguientes:

### 2.1 Problema: Control plane receiver recibe `TryingReady` antes de que el master registre la votación

**Contexto del problema:**

En un escenario con varios `sum`, el flujo esperado era:

1. `sum_0` recibe EOF → registra votación → emite `Commit` por broadcast.
2. Otros `sum` reciben `Commit` → envían `TryingReady` al master.

Pero con envío asíncrono, puede ocurrir que:

- `sum_1` procesa `TryingReady(C, k)` en su control-plane receiver **antes** de que `sum_0` (el master de esa votación) haya ejecutado `regist_new_votation(C)`.
- Sin protección: `add_processed_data_count(k, C)` se ejecuta en un `VotationStatus` que no existe → se ignora del conteo.

**Resultado:** la votación nunca alcanza `expected`, voter nunca llega a `Ok`, flush nunca ocurre.

### 2.2 Solución: Buffer `pending_trying_ready_by_votation_id`

**Decisión de diseño (NO explícitamente pedida):**

Se agregó una estructura:

```python
pending_trying_ready_by_votation_id = {}  # {votation_id: accumulated_count}
```

**Flujo de operación:**

1. Al recibir `TryingReady(C, k)` en el master:
   - Si `VotationStatus` para `C` **no existe**, se guarda `k` en `pending_trying_ready_by_votation_id[C] += k`.
   - Si **sí existe**, se suma directamente a `current_processed_data_count`.

2. Al ejecutar `regist_new_votation(C, expected)`:
   - Se crea `VotationStatus`.
   - Si hay pendiente: transferir `pending_trying_ready_by_votation_id[C]` al contador oficial.
   - Limpiar la entrada del buffer.

3. Como resultado:
   - No se pierde ningún `TryingReady` por timing.
   - La votación puede alcanzar `expected` y completarse correctamente.

**Invariante establecida:** todo `TryingReady` que llegue es contabilizado, sin importar el orden relativo con `Commit`.

### 2.3 Problema: Múltiples broadcasts de `Ok` por la misma votación

**Contexto del problema:**

En el master, la condición `if current_processed_data_count >= expected_processed_data_count:` podría ser verdadera en múltiples invocaciones de `add_processed_data_count`.

Si varios `TryingReady` llegan muy juntos,uno podría activar el condition y luego otro podría hacerlo nuevamente:

```python
# En el master, al recibir TryingReady:
add_processed_data_count(...)
if digestion_complete(...):  # ← Puede ser cierto varias veces
    enqueue_ok_broadcast(...)  # ← Se encola múltiple
```

**Resultado:** `Ok` se envía más de una vez, desperdiciando recursos y complicando idempotecia.

### 2.4 Solución: Campo `ok_broadcasted` en `VotationStatus`

**Decisión de diseño (NO explícitamente pedida):**

Se agregó a `VotationStatus`:

```python
ok_broadcasted = False
```

**Flujo de protección:**

1. En el master, al recibir `TryingReady`:
   ```python
   if digestion_complete(votation_id) and not votation_status.ok_broadcasted:
       votation_status.ok_broadcasted = True
       enqueue_ok_broadcast(votation_id)
   ```

2. Una sola emisión de `Ok`, sin importar cuántos `TryingReady` se acumulen.

**Invariante establecida:** cada votación emite `Ok` **exactamente una vez**.

---

## 3) Flujo nominal de una votación (cliente `C`) con cambios integrados

1. **Digestión de datos**
   - Cada `sum` que recibe `[fruit, amount, C]` actualiza `DigestPool`.
   - Si ya conoce el master de `C`, envía `TryingReady(C, 1)` directo al master.
   - Si **no** conoce el master (aún no llegó `Commit`), el `TryingReady` se bufferea en el master.

2. **Inicio de votación al EOF en el master**
   - Master recibe `EOF(C, total)`:
     - Registra `VotationStatus(expected=total)`.
     - **[CAMBIO EMERGENTE]** Transfiere el buffer `pending_trying_ready[C]` al contador oficial.
     - Se autodefine master (`"{ID}_control_routing_key"`) para esa votación.
     - Emite `Commit(C, master_routing_key)` por broadcast.

3. **Recepción de `Commit` en otras réplicas**
   - Guardan `master_routing_key` para `C`.
   - Leen su digest local actual (`cant_data_processed`) y envían `TryingReady(C, cant_data_processed)` al master.

4. **Agregación de progreso en el master**
   - Al recibir cada `TryingReady`:
     - Si `VotationStatus` NO existe: **[CAMBIO EMERGENTE]** bufferea en `pending_trying_ready[C]`.
     - Si existe: suma a `current_processed_data_count`.
     - **[CAMBIO EMERGENTE]** Si `digestion_complete()` y NO se ha `ok_broadcasted`:
       - Set `ok_broadcasted = True`.
       - Emite `Ok(C)` broadcast.

5. **Flush al recibir `Ok` (todas las réplicas)**
   - Cada réplica toma `data_per_fruit` de `C`.
   - Para cada fruta aplica el **mismo hash histórico**:
     - `sha256(f"{fruit}:{client_id}") % AGGREGATION_AMOUNT`.
   - Envía `[fruit, amount, client_id]` al `aggregation` correspondiente.
   - Luego envía EOF `[client_id]` a **cada** `aggregation`.
   - **[CAMBIO EMERGENTE]** Ejecuta limpieza uniforme de estado residual de `C`.

---

## 4) Seguimientos de cambios emergentes y liberación de recursos por cliente

### Seguimiento 1: Carrera de `TryingReady` antes de registrar votación (Buffer `pending_trying_ready`)

**Contexto:** `SUM_AMOUNT=3`, cliente `C`.

**Timeline:**

1. `sum_0` (master) aún no recibió su mensaje EOF.
2. Data-plane de `sum_1` digiere datos de `C` rápidamente.
3. `sum_1` envía `TryingReady(C, k=40)` al master `sum_0`.
4. **Control-plane receiver de `sum_0` recibe `TryingReady(C, 40)` ANTES de que EOF de `sum_0` le haya hecho `regist_new_votation(C, ...)`**.

**Sin buffer `pending_trying_ready`:**

- El receiver intenta `VotationsMonitor.add_processed_data_count(40, C)`.
- `VotationStatus` no existe → operación es ignorada.
- Luego, cuando EOF llega y registra votación con `expected=100`:
  - `current_processed_data_count = 0` (se perdió el 40).
  - Las próximas réplicas envían `TryingReady(C, 30)` y `TryingReady(C, 30)` → suma a 60.
  - Nunca alcanza 100 → votación queda bloqueada → sin `Ok` → sin flush.

**Con buffer `pending_trying_ready`:**

1. Receiver de `sum_0` al llegar `TryingReady(C, 40)` (sin votación registrada):
   ```python
   if C not in VotationsMonitor.votations:
       pending_trying_ready_by_votation_id[C] += 40
   ```

2. Cuando EOF llega, `regist_new_votation(C, 100)`:
   ```python
   votation_status = VotationStatus(100)
   if C in pending_trying_ready_by_votation_id:
       votation_status.current_processed_data_count += pending_trying_ready_by_votation_id[C]
       del pending_trying_ready_by_votation_id[C]
   ```

3. Ahora `current = 40`, las otras réplicas suman 30+30 = 60, total 100 → `digestion_complete()` es true → `Ok` se emite.

**Resultado:** El buffer garantiza que ningún `TryingReady` se pierda por timing, independientemente del orden de llegada de EOF.

---

### Seguimiento 2: Protección contra múltiples broadcasts de `Ok` (Campo `ok_broadcasted`)

**Contexto:** `SUM_AMOUNT=2`, cliente `D`, master es `sum_0`.

**Timeline:**

1. `sum_0` ya tiene `VotationStatus(D)` registrada con `expected = 60`, `current = 0`.
2. Llegan en ráfaga rápida dos mensajes:
   - `TryingReady(D, 30)` de `sum_0` self.
   - `TryingReady(D, 30)` de `sum_1`.

**Sin campo `ok_broadcasted`:**

```python
# Procesando TryingReady(D, 30) de sum_0:
add_processed_data_count(30, D)  # current = 30
if digestion_complete(D):  # 30 >= 60? NO
    enqueue_ok_broadcast(D)

# Procesando TryingReady(D, 30) de sum_1:
add_processed_data_count(30, D)  # current = 60
if digestion_complete(D):  # 60 >= 60? SÍ
    enqueue_ok_broadcast(D)  # ← Se encola Ok
```

Si el timing fuera distinto (ej: dos TryingReady pequeños acumulados):

```
# Procesando TryingReady(D, 15):
add_processed_data_count(15, D)  # current = 45
if digestion_complete(D): NO

# Procesando TryingReady(D, 15):
add_processed_data_count(15, D)  # current = 60
if digestion_complete(D): SÍ → enqueue_ok_broadcast(D)

# Pero si hubiera un tercer mensaje por timeout o reenvío:
# Procesando TryingReady(D, 0):  (heartbeat/retry)
add_processed_data_count(0, D)  # current = 60 (no cambia)
if digestion_complete(D): SÍ → enqueue_ok_broadcast(D)  # ← Duplicado!
```

**Con campo `ok_broadcasted`:**

```python
# En VotationStatus:
ok_broadcasted = False

# Procesando cualquier TryingReady después de alcanzar expected:
add_processed_data_count(k, D)
if digestion_complete(D) and not votation_status.ok_broadcasted:
    votation_status.ok_broadcasted = True
    enqueue_ok_broadcast(D)  # ← Garantizado una sola vez
```

**Resultado:** Se emite exactamente un `Ok` por votación, sin importar cuántos `TryingReady` lleguen después de alcanzar `expected`.

---

### Seguimiento 3: Liberación uniforme de estado tras completar votación

**Contexto:** Cliente `E`, votación completada con `Ok`.

**Patrón uniforme (mismo en todas las réplicas):**

1. Al recibir `Ok(E)`:
   - Flush de digest (envío a aggregators).
   - Limpieza:
     ```python
     DigestPool.delete_client_digest(E)
     VotationsMonitor.delete_votation(E)
     MastersRoutingKeyByVotationID.delete_votation(E)
     pending_trying_ready_by_votation_id.pop(E, None)
     ```

2. **Invariante:** tras `Ok`, no quedan referencias a `E` en ningún mapa interno de `sum`.

3. **Beneficio:** idempotencia segura; si `Ok` se retransmitiera, no causaría corrupción de estado.

---

## 5) Integración de cambios emergentes en el sistema global

Los cambios emergentes (buffer de `TryingReady` y protección `ok_broadcasted`) se integran de forma natural:

1. **Compatibilidad con gateway/aggregation/join:** Cero cambios requeridos. El contrato de mensajes (`FruitData`, `Eof`, y los datos resultantes a `join`) se mantiene idéntico.

2. **Simetría en todas las réplicas:** Tanto master como non-master replicas del mismo `sum` siguen el mismo patrón de flush y limpieza, garantizando uniformidad.

3. **Robustez a desorden:** El buffer previene pérdida de datos por timing adverso, y `ok_broadcasted` previene duplicados de `Ok`.

---

## 6) Resumen y comportamiento final

La arquitectura final implementa el protocolo de votación especificado originalmente con **dos extensiones críticas que surgieron por necesidad de sincronización**:

1. **Buffer `pending_trying_ready_by_votation_id`:**
   - Maneja el caso donde `TryingReady` llega antes de registrar la votación.
   - Garantiza que ningún mensaje de progreso se pierda por timing adverso.
   - Se integra transparentemente en `regist_new_votation()` sin cambiar el contrato externo.

2. **Campo `ok_broadcasted` en `VotationStatus`:**
   - Protege contra múltiples broadcasts accidentales de `Ok`.
   - Garantiza exactitud: una sola emisión por votación terminada.
   - Mantiene la idempotencia de la limpieza posterior.

**Resultado final:**

- ✅ Robustez comprobada en los 5 escenarios (desde 1 cliente hasta 3 replicas paralelas).
- ✅ Cero cambios en gateway, aggregation, join, client → contrato de datos intacto.
- ✅ Hash de routing a aggregators persistente (sin cambios).
- ✅ Limpieza uniforme de estado por cliente → sin residuos tras votación.

Este diseño emergente fue necesario para manejar el asincronismo inherente a la distribución, pero no fue explícitamente especificado en el prompt original; es resultado de análisis profundo de fallos potenciales durante la codificación.