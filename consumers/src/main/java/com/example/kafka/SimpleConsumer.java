package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/* **
 *  <kafka 토픽 생성 및 확인>
 *   - 토픽 생성
 *      kafka-topics --bootstrap-server localhost:9092 --delete --topic simple-topic
 *      kafka-topics --bootstrap-server localhost:9092 --create --topic simple-topic
 *   - 메시지전송 테스트
 *      kafka-console-producer --bootstrap-server localhost:9092  --topic simple-topic
 *   - 메시지수신 테스트
 *      kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-topic
 *      kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-topic --from-beginning
 * */

public class SimpleConsumer {

    public static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class.getName());

    public static void main(String[] args) {

        String topicName = "simple-topic";

        /* 환경설정 *************************************************************************************************** */
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01");

        // HEARTBEAT 설정
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,"5000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "90000");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");


        /* 환경설정 *************************************************************************************************** */

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(List.of(topicName));

        // Consumer가 poll하기...
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : consumerRecords) {
                logger.info("record key:{}, record value:{}, partition:{}",
                        record.key(), record.value(), record.partition());
            }
        }

        //kafkaConsumer.close();

    }
}







