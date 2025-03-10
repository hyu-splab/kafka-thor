package com.yourcompany;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map.Entry;


public class DynamicServiceRouter {
    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("k_cars");
        ObjectMapper objectMapper = new ObjectMapper();
        String[] topics = {"Sensor_Sharing","Information_Sharing","Platooning_Lowest","Platooning_Lower"};
        sourceStream.flatMap((key, value) -> {
            List<KeyValue<String, String>> results = new ArrayList<>();
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                String serviceName = jsonNode.has("service_name") ? jsonNode.get("service_name").asText() : null;
                if(serviceName != null){
                    char firstChar = serviceName.charAt(0);
                    if(Character.isDigit(firstChar)){
                        int index = Character.getNumericValue(firstChar);
                        if (index >= 0 && index < topics.length) {
                            String topic = topics[index];
                            results.add(new KeyValue<>(topic, value));
                        }
                    }else{
                        String topic = "Processed";
                        results.add(new KeyValue<>(topic, value));
                    }
                }else{
                    System.err.println("Warning: service_name field is missing in the record");
                }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return results;
                })
                .peek((key, value) -> {
             })
            .to((key, value, recordContext) -> key); // Dynamic topic naming based on service key
        KafkaStreams streams = new KafkaStreams(builder.build(), properties());
        streams.start();
        // Add shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
    private static Properties properties() {
        Properties props = new Properties();
        props.put("application.id", "dynamic-service-router");
        props.put("bootstrap.servers", "192.168.0.1:9092");
        props.put("default.key.serde", Serdes.String().getClass().getName());
        props.put("default.value.serde", Serdes.String().getClass().getName());
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "dynamic-service-router");
        props.put("auto.offset.reset", "latest");
        props.put("producer.acks", "0");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        props.put("client.id","dynamic-service-router");
        // props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return props;
    }
}
