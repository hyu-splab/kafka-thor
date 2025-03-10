package com.example.kafka.smt;

import org.apache.kafka.connect.connector.ConnectRecord;
//import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.common.config.ConfigDef;
//import org.apache.kafka.common.utils.AppInfoParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.Map;

//import java.text.SimpleDateFormat;
//import java.util.Date;

public class extractService<R extends ConnectRecord<R>> implements Transformation<R>  {

      private static final Logger log = LoggerFactory.getLogger(extractService.class);

    public static final String OVERVIEW_DOC = "Update the record topic using service_name field.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    //SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    //int count = 0;

    @Override
    public void configure(Map<String, ?> props) {
        //현재 추가할거 없음 
        //regexRouter로 따지면, "transforms.dropPrefix.regex","transforms.dropPrefix.replacement"에서 
        //regex,replacemet 정의하는거 
    }

    @Override
    public R apply(R record) {

        //count++;
        //String currentTime = dateFormat.format(new Date());
        //System.out.println("SMT: 10:" + count + ": " + currentTime);

        //record 확인
        if(!(record.value() instanceof byte[])){
            log.warn("Not byte[]");
            System.out.println("Record value is not of type byte[], found type: " +
                       (record.value() != null ? record.value().getClass().getName() : "null") +
                       ", value: " + record.value());
            return record;
        }
        
        String jsonString = null;

        try {
            // byte[]를 String으로 변환
            byte[] byteValue = (byte[]) record.value();
            jsonString = new String(byteValue, StandardCharsets.UTF_8);
        } catch (Exception e) {
            System.out.println("Failed to convert byte[] to String. Error: " + e.getMessage());
            System.out.println("byte[] : "+
            (record.value() != null ? record.value().getClass().getName() : "null") +
            ", value: " + record.value());
            e.printStackTrace(); // 예외 스택 트레이스 출력
            return record; // 변환 실패 시 원래 레코드를 반환
        }


        try {
            // String을 JSON으로 파싱
            JSONObject jsonObject = new JSONObject(jsonString);
            log.trace("Parsed JSON Object: {}", jsonObject.toString());

            // service_name 필드의 값을 추출
            if (jsonObject.has("service_name")) {
                String serviceName = jsonObject.getString("service_name");
                log.info("Extracted 'service_name': {}", serviceName);

                // service_name의 첫 번째 글자에 따라 토픽 설정
                String newTopic;
                switch (serviceName.charAt(0)) {
                    case '0':
                        newTopic = "Sensor_Sharing";
                        break;
                    case '1':
                        newTopic = "Information_Sharing";
                        break;
                    case '2':
                        newTopic = "Platooning_Lowest";
                        break;
                    case '3':
                        newTopic = "Platooning_Lower";
                        break;
                    default:
                        newTopic = "k_cars";
                        break;
                }

                log.info("Rerouting record to new topic '{}'", newTopic);

                // 새로운 레코드 반환
                return record.newRecord(
                        newTopic,
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        record.valueSchema(),
                        record.value(),
                        record.timestamp()
                );
            } else {
                log.warn("The 'service_name' field is not present in the record");
            }
        } catch (Exception e) {
            log.error("Failed to parse JSON from record", e);
        }

        // JSON 파싱 실패 또는 service_name 필드가 없는 경우 원래 레코드 반환
        return record;

    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}
