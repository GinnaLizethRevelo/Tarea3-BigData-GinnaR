# Tarea 3 – Procesamiento de Datos con Apache Spark

**Estudiante:** Ginna Lizeth Revelo Narvaez  
**Curso:** Big Data  
**Código:** 202016911  
**Universidad:** Universidad Nacional Abierta y a Distancia – UNAD  

## Descripción

Este repositorio contiene los archivos desarrollados para la Tarea 3 del curso Big Data.

La práctica incluye dos tipos de procesamiento:

1. **Procesamiento batch con Apache Spark**, utilizando el conjunto de datos histórico de la Tasa Representativa del Mercado (TRM) de Colombia.
2. **Procesamiento en tiempo real con Apache Kafka y Spark Streaming**, utilizando datos simulados de sensores.

## Estructura del repositorio

- `tarea3_batch.py`: procesamiento batch de la TRM con Apache Spark.
- `kafka_producer.py`: productor que genera datos simulados de sensores y los envía al topic `sensor_data`.
- `spark_streaming_consumer.py`: consumidor que utiliza Spark Streaming para procesar los mensajes recibidos desde Kafka.
- `requirements.txt`: dependencia de Python utilizada por el productor de Kafka.

## Dataset utilizado en el procesamiento batch

Se utiliza el conjunto de datos **Tasa de Cambio Representativa del Mercado – Histórico**, disponible en el Portal de Datos Abiertos de Colombia.

El archivo CSV se almacena en HDFS en la siguiente ruta:

```text
/Tarea3/rows.csv
```

El archivo contiene las columnas:

```text
VALOR
UNIDAD
VIGENCIADESDE
VIGENCIAHASTA
```

## Ejecución del procesamiento batch

Primero se deben iniciar los servicios de Hadoop:

```bash
su - hadoop
start-all.sh
```

Verificar el archivo en HDFS:

```bash
hdfs dfs -ls /Tarea3
```

La ruta esperada es:

```text
/Tarea3/rows.csv
```

Ejecutar el código batch:

```bash
spark-submit tarea3_batch.py
```

El programa realiza:

- lectura del CSV desde HDFS;
- visualización del esquema;
- visualización de registros;
- eliminación de duplicados;
- eliminación de registros incompletos;
- conversión del valor de la TRM a formato numérico;
- conversión de fecha;
- estadísticas descriptivas;
- cálculo de promedio, mínimo y máximo;
- filtro de valores superiores a 5.000 COP;
- identificación de los valores más altos;
- cálculo del promedio anual de la TRM.

## Ejecución de Kafka y Spark Streaming

Instalar la dependencia de Python:

```bash
pip install kafka-python
```

Iniciar ZooKeeper:

```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
```

Iniciar Kafka:

```bash
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
```

Crear el topic:

```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
```

Ejecutar el productor:

```bash
python3 kafka_producer.py
```

En otra terminal ejecutar el consumidor:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```

## Resultados esperados

En el procesamiento batch se obtienen estadísticas y tendencias históricas de la TRM.

En el procesamiento en tiempo real, el productor genera datos simulados de temperatura y humedad. Kafka recibe los eventos mediante el topic `sensor_data` y Spark Streaming los procesa para calcular promedios por sensor en ventanas de un minuto.
