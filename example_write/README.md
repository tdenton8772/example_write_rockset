# Rockset Java Store and Forward Example

This is an example of how to combine the Rockset WriteAPI and Kafka to ensure writes are always stored.

## Requirements
- Maven
- Java 11 (may run with Java 8 but that is untested)

## Usage

Edit the `configuration.properties` file for your environment. Remember, this is an example and you should not store API Keys in plain text files for production environments.

```bash
API_SERVER=<API Server>
API_KEY=<APIKEY>
workspace=<Workspace that contains your collection>
collection=<collection to write to>
max_retries=<How many times you want to retry the write>
write_threads=<number of threads to start writing>
topic=<kafka topic for failures>
body_length=<size of document body>
```

Edit the kafka.properties file for your environment. In this example we do no implement access control. The only value you need to update is the bootstrap.servers list.

```bash
bootstrap.servers=<address:9092,address:9092>
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

After those files are updated you car build and run the JAR file.

```bash
mvn clean package
java -jar ./target/example_write-1.0-SNAPSHOT.jar
```