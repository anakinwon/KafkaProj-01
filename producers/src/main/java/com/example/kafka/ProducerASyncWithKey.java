package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

    /* *********************************************************************
        1. 토픽생성
           토픽 삭제 : kafka-topics --bootstrap-server localhost:9092 --delete --topic multipart-topic
           토픽 생성 : kafka-topics --bootstrap-server localhost:9092 --create --topic multipart-topic --partitions 3

        2. 전송완료 후 kafka topic 메시지 확인하기
           kafka-console-consumer --bootstrap-server localhost:9092 --group group-01 --topic multipart-topic --property print.key=true --property print.value=true
     * */

public class ProducerASyncWithKey {
    public static final Logger logger = LoggerFactory.getLogger(ProducerASyncWithKey.class.getName());
    public static void main(String[] args) {

        String topicName = "multipart-topic";

        //KafkaProducer configuration setting
        // null, "hello world"

        Properties props  = new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        //props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        for(int seq=3000; seq <= 4001; seq++) {
            //ProducerRecord object creation
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq),"400. ProducerASyncWithKey Call " + seq);
            logger.info("seq:" + seq);
            //kafkaProducer message send
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("\n ###### record metadata received ##### \n" +
                            "partition:" + metadata.partition() + "\n" +
                            "offset:" + metadata.offset() + "\n" +
                            "timestamp:" + metadata.timestamp());
                } else {
                    logger.error("exception error from broker " + exception.getMessage());
                }
            });
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
           e.printStackTrace();
        }

        kafkaProducer.close();

    }

}