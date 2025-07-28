package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/* **
 *  <kafka 토픽 생성 및 확인>
 *      kafka-topics --bootstrap-server localhost:9092 --delete --topic simple-topic
 *      kafka-topics --bootstrap-server localhost:9092 --create --topic simple-topic
 *      kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-topic --from-beginning
 * */

public class SimpleProducer {
    public static void main(String[] args) {

        String topicName = "simple-topic";

        //KafkaProducer configuration setting
        // null, "hello world"

        Properties props  = new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        //props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer object creation (객체 생성)
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        //ProducerRecord object creation (토픽 생성)                        //topicName = "simple-topic";
        ProducerRecord<String, String> producerRecord ;

         for (int i=1; i<=100; i++) {
             producerRecord = new ProducerRecord<>(topicName,"100. SimpleProducer Send "+i);

             //KafkaProducer message send (전송을 위한 호출)
             kafkaProducer.send(producerRecord);
         }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
    /* *********************************************************************
     * 전송완료 후 kafka topic 메시지 확인하기
        : kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-topic --from-beginning
     * */
}
