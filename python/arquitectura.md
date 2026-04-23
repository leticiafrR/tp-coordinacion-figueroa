# Informe de arquitectura del sistema (pipeline distribuido)

## 1) Resumen ejecutivo

Este sistema implementa un pipeline distribuido para procesar datasets de frutas por cliente, agregando resultados de forma determinística y tolerando despliegue con réplicas en etapas intermedias.

Flujo de alto nivel:

1. `client` lee CSV y envía datos al sistema.
2. `gateway` recibe conexiones de clientes y publica eventos al broker.
3. `sum` consume eventos, coordina réplicas y decide el momento de flush a los nodos de *aggregation*.
4. `aggregation` consolida y calcula el top solicitado.
5. `join` arma respuesta final por cliente y la retorna vía `gateway`.

El middleware de mensajería es RabbitMQ y la comunicación se divide en:

- **Data plane:** mensajes de datos y EOF funcionales.
- **Control plane:** coordinación interna de réplicas `sum` (`Commit`, `TryingReady`, `Ok`).

---

## 2) Componentes y responsabilidades

### `client`

- Lee `INPUT_FILE`.
- Envía registros al `gateway`.
- Espera y guarda salida en `OUTPUT_FILE`.

### `gateway`

- Expone endpoint de entrada/salida para clientes.
- Publica datos de entrada a la cola de procesamiento.
- Reenvía resultados finales de vuelta al cliente correspondiente.

### `sum` (coordinado entre réplicas)

- Acumula conteos por cliente y fruta (`DigestPool`).
- Coordina réplica master por transacción para decidir cuándo cerrar el lote.
- Envia resultados parciales a `aggregation` empleando hash de ruteo.

### `aggregation`

- Recibe pares agregados por fruta/cliente desde `sum`.
- Consolida información recibida entre réplicas.
- Emite resultados hacia `join`.

### `join`

- Reúne resultados provenientes de `aggregation`.
- Ensambla respuesta final por cliente.
- Publica resultado en la cola de salida consumida por `gateway`.

### `rabbitmq`

- Transporte de mensajes para data plane y control plane.
- Permite desacople entre productores y consumidores.

---

## 3) Flujo funcional de extremo a extremo

1. `client` envía `FruitData` y EOF lógico asociado al cliente.
2. `gateway` publica hacia la cola de entrada de `sum`.
3. Cada réplica `sum` digiere datos localmente por `client_id`.
4. Ante EOF, se inicia coordinación de cierre por transacción en `sum`:
   - se define master,
   - se broadcastea `Commit`,
   - cada réplica reporta progreso con `TryingReady`.
5. El master emite `Ok` al detectar completitud.
6. Todas las réplicas `sum` flushean su digest a `aggregation`.
7. `aggregation` procesa y envía a `join`.
8. `join` consolida y publica resultado final.
9. `gateway` devuelve la respuesta al cliente.

---

## 4) Coordinación de réplicas en `sum`

Cada nodo `sum` ejecuta tres loops lógicos:

1. **Data-plane consumer:** consume cola de entrada (`INPUT_QUEUE`).
2. **Control-plane receiver:** consume mensajes de control en `SUM_CONTROL_EXCHANGE`.
3. **Control-plane sender:** drena una cola interna y publica control de forma serializada.

Estado por transacción (`client_id`):

- `DigestPool`: acumulación local por fruta y contador de ítems procesados.
- `TransactionsMonitor`: conteo esperado vs procesado para decidir completitud.
- `MastersRoutingKeyByTransactionId`: mapeo de master activo por transacción.

Protocolo principal:

1. El master inicia transacción con `Commit(transaction_id, master_routing_key)`.
2. Réplicas responden `TryingReady(transaction_id, amount_fruits_processed)`.
3. Nuevos datos post-`Commit` también emiten `TryingReady(..., 1)`.
4. El master evalúa completitud y emite `Ok(transaction_id)` cuando `processed_count == expected_count`



5. Con `Ok`, cada réplica hace flush a `aggregation`, envía EOF a todas las colas de aggregation y limpia estado local.

---

## 5) Cierre limpio en los `sum`

En esta sección procedemos a explicar brevemente el proceso que se sigue para apagar correctamente los procesos sums, los demás nodos del sistema no requieren un lógica compleja para llegar al grafecul shutdown dado que el único recurso bloqueante que se usa son los middlewares (aqui no se requiere una comunicación tan fuerte). Por otro lado, gran parte del procedimiento seguido para realizar shutdown se basó al código preexistente que ya contaba con un graceful shutdown (clients y gateway) así mismo como lo discutido en clases en retrospectiva al tp0.

### 5.1 Cómo se realiza

El cierre limpio se centraliza en `request_shutdown()` del nodo `sum` y es idempotente:

- usa lock (`_shutdown_lock`) y flag (`_shutdown_started`) para ejecutar el cierre una sola vez;
- desactiva el loop principal (`keep_running = False`);
- detiene consumo de data plane (`data_queue.stop_consuming()`);
- detiene control plane receiver (`control_plane_receiver.stop()`);
- detiene control plane sender (`control_plane_sender.stop()`), lo que desbloquea su loop interno;
- finalmente, el main thread de sum espera finalización de threads (`join`) y luego cierra recursos de mensajería.

### 5.2 En qué momento ocurre

El cierre se dispara en dos caminos principales:

1. **Cierre esperado (graceful):**
   - recepción de `SIGTERM` (handler registrado en `main()`), o
   - salida natural del flujo de ejecución que entra al bloque `finally`.

2. **Cierre por error (no esperado):**
   - excepción en `start()` con `keep_running` todavía en `True`.
   - se loguea error y se retorna código `1`, pero igual se ejecuta limpieza en `finally`.

### 5.3 Qué provoca y cómo se maneja

Efectos del cierre limpio:

- evita nuevos envíos al sender de control (`_stopped = True`);
- corta consumidores de RabbitMQ para no seguir recibiendo mensajes;
- evita fugas de recursos cerrando exchanges/colas;
- reduce condiciones de carrera con estrategia de parada única + joins.



## 6) Requisitos de runtime

**Versión mínima recomendada de python: Python 3.13+**: el nodo `sum` usa `queue.Queue.shutdown()` y el tipo `queue.ShutDown` para finalizar el sender de control de forma segura. Estas APIs no están disponibles en versiones antiguas de Python.

