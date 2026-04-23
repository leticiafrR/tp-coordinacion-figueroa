# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.


## Informe de arquitectura y evolución de la solución
- De: Leticia Antuaned Figueroa Rodriguez 
- padrón: 110510

Esta solución parte del contrato del TP: un `client` lee un CSV, lo envía al `gateway`, el sistema procesa los datos a través de `Sum`, `Aggregation` y `Join`, y finalmente devuelve al cliente un top agregado. El foco principal de la implementación fue lograr que el sistema funcione correctamente en escenarios con múltiples clientes y múltiples réplicas, manteniendo consistencia y evitando trabajo duplicado.

### 1. Problemas del esqueleto inicial

El diseño inicial no resolvía correctamente escenarios concurrentes:

* No existía un aislamiento claro por cliente.
* El manejo del EOF era incorrecto: no todos los nodos `Sum` sabían cuándo debían flushear.
* Los `Sum` hacían broadcast hacia todos los `Aggregation`, duplicando datos y procesamiento.

Esto impedía escalar más allá de un cliente o una sola réplica por componente.

---

### 2. Evolución de la solución

Primero se introdujo identificación por `client_id`, permitiendo que múltiples clientes procesen en paralelo sin interferencias.

Luego se separó el sistema en dos planos:

* **Data plane**: transporte de datos (`Fruit`, `EOF`)
* **Control plane**: coordinación entre nodos (`Commit`, `TryingReady`, `Ok`)

Finalmente, se reemplazó el broadcast por un esquema de particionado con hash, donde cada `(cliente, fruta)` se asigna de forma determinística a un nodo `Aggregation`. Esto elimina duplicación y distribuye mejor la carga.

---

### 3. Decisión de particionado

Se eligió distribuir por `(cliente, fruta)` porque:

* Solo por fruta → poca granularidad si hay pocas frutas
* Solo por cliente → riesgo de sobrecargar nodos con clientes grandes

La combinación balancea ambos problemas y permite escalar mejor.

---

### 4. Coordinación en `Sum`

Los nodos `Sum` procesan datos en paralelo pero coordinan el cierre de cada cliente mediante el control plane.

Cuando un nodo detecta `EOF` para un cliente:

1. Inicia una transacción enviando `Commit`
2. Los demás nodos responden con cuánto procesaron (`TryingReady`)
3. Cuando se alcanza el total esperado, se emite `Ok`
4. Recién ahí todos los nodos flushean sus resultados hacia `Aggregation`

Esto evita que algún nodo flushee prematuramente.

---

### 5. `Aggregation` y `Join`

* `Aggregation` recibe parciales ya particionados, acumula y genera un top intermedio.
* `Join` reúne esos resultados y arma el resultado final por cliente.
* El `Gateway` devuelve la respuesta al cliente.

---

### 6. Modelo de concurrencia (threads)

Se utilizan threads para manejar:

* consumo de datos
* recepción de mensajes de control
* envío de mensajes de control

El sistema es principalmente **I/O bound** (espera mensajes de RabbitMQ), por lo que los threads permiten intercalar esas esperas de forma eficiente.

Limitación importante: en Python existe el **GIL**, lo que implica que los threads no ejecutan en paralelo real a nivel CPU. Sin embargo, en este caso no es un problema porque:

* el tiempo dominante es espera de I/O
* los threads liberan el GIL al bloquearse
* el tráfico de control es bajo respecto al de datos

Esto permite un modelo simple y suficiente sin necesidad de multiprocessing.

---

### 7. Escalabilidad

El sistema escala en dos dimensiones:

* **Clientes**: cada `client_id` se procesa de forma independiente
* **Réplicas**:

  * `Sum` escala consumiendo en paralelo de la cola
  * `Aggregation` escala mediante particionado por hash

Esto permite aumentar throughput agregando instancias sin introducir inconsistencias.

---

## Apéndice A: Coordinación distribuida

### Topología

Los nodos `Sum` no se conectan directamente entre sí. Toda la coordinación ocurre a través de RabbitMQ, formando una **topología lógica en estrella**:

```
        Sum_1
          |
Sum_2 — RabbitMQ — Sum_3
          |
        Sum_n
```

Esto desacopla completamente a los nodos y simplifica la comunicación.

---

### Modelo de transacción

La coordinación está inspirada en **2-Phase Commit**, pero adaptada:

* Se usa para decidir **cuándo flushear**, no para garantizar atomicidad fuerte.
* No hay rollback, solo sincronización de cierre.

Flujo simplificado:

1. Un nodo detecta EOF → envía `Commit`
2. Los nodos responden con su progreso (`TryingReady`)
3. Cuando todos alcanzaron el estado esperado → se envía `Ok`
4. Todos flushean

---

### Componentes del control plane

#### Sender

Thread encargado únicamente de enviar mensajes:

* puede hacer broadcast
* puede enviar a una routing key específica

No toma decisiones, solo ejecuta órdenes de otros componentes.

---

#### Receiver

Thread que:

* escucha en su routing key
* procesa mensajes con callbacks
* delega envíos al sender cuando es necesario

Se mantiene bloqueado consumiendo mensajes continuamente.

---

#### Data plane (Sum)

Procesa:

* `Fruit`: acumula datos y reporta progreso si hay una transacción activa
* `EOF`: inicia el proceso de coordinación (`Commit`)



## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.
