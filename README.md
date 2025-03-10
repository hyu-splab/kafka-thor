# **kafka_thor**


## **Prerequisites**
- Download **Mosquitto** at https://github.com/eclipse-mosquitto/mosquitto
- Downlaod **Apache Kafka** at https://github.com/apache/kafka
- Download **RapidJSON** for `curator` at https://github.com/Tencent/rapidjson
- Download **nlohmann/json** for `Multi` (testing program) at https://github.com/nlohmann/json
- Compile and install **librdkafka** (used in this repository)

```bash
# Install librdkafka
cd librdkafka
./configure
make
make install

# Compile executables 
cd curator
make -j

# Kafka Ecosystem Setup
cd kafka_ecosystem/confluent-kafka-connect-mqtt/lib
gzip -d ./*.jar
```
For Different Kafka Implementations
1. Kafka Streams
 - MQTT Connector: mqtt-source-connector.json
 - Kafka Stream Application: DynamicServiceRouter.class
 - Testing Program: curator/Multi_Stream
2. Kafka SMT (Single Message Transform)
 - MQTT Connector: smt-mqtt-source-connector.json
 - Testing Program: curator/src/Multi_SMT
3. Kafka-Thor
 - Main Program: curator/Thor
 - Testing Program: curator/Multi_Thor

For all above, use curator/Vehicle to simulate othger cars
