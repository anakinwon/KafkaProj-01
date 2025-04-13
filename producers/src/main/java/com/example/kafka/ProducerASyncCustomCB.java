package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
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


public class ProducerASyncCustomCB {
    public static final Logger logger = LoggerFactory.getLogger(ProducerASyncCustomCB.class.getName());
    public static void main(String[] args) {

        String topicName = "multipart-topic";

        //KafkaProducer configuration setting
        // null, "hello world"

        Properties props  = new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        //props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer object creation
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);

        for(int seq=4000; seq <= 5001; seq++) {
            //ProducerRecord object creation
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq,"500. ProducerASyncCustomCB Call " + seq);
            CustomCallback callback = new CustomCallback(seq);
            //logger.info("seq:" + seq);
            //kafkaProducer message send
            kafkaProducer.send(producerRecord, callback);
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
           e.printStackTrace();
        }

        kafkaProducer.close();

    }
}