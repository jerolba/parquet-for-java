# Parquet for Java

Sample code of "Parquet for Java" talk.

Load data using:
* Apache Avro:
    * Generic Records
    * Reflection
    * Generated Code
* Protocol Buffers
* Carpet
* DuckDB

Write data using:
* Apache Avro:
    * Generic Records
    * Reflection
    * Generated Code
* Protocol Buffers
* Carpet

## Commands

To fetch sample data set:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet
```

To generate Protocol buffers classes

```bash
docker run --rm -v $(pwd):$(pwd) -w $(pwd) znly/protoc --java_out=./src/main/java -I=./src/main/resources ./src/main/resources/trips.proto2
```

To generate Avro classes:

```bash
java -jar avro-tools-1.10.0.jar compile schema ./src/main/resources/organizations.avsc ./src/main/java/
```

```bash
docker run --rm -v $(pwd)/src:/avro/src kpnnv/avro-tools:1.10.0 compile schema /avro/src/main/resources/trips.avsc /avro/src/main/java/
```
