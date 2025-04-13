package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

    /* *********************************************************************
        1. 토픽 생성하기
           토픽 삭제 : kafka-topics --bootstrap-server localhost:9092 --delete --topic pizza-topic
           토픽 생성 : kafka-topics --bootstrap-server localhost:9092 --create --topic pizza-topic --partitions 3

        2. 전송완료 후 kafka topic 메시지 확인하기
           kafka-console-consumer --bootstrap-server localhost:9092 --group group-01 --topic pizza-topic --property print.key=true --property print.value=true
             => 동일한 스크립트로 3개에 창으로 실행하면, 분산작업이 진행된다.
     * */

public class PizzaProducer {
    public static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class.getName());


    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount,
                                        int interIntervalMillis, int intervalMillis,
                                        int intervalCount, boolean sync) {

        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        long startTime = System.currentTimeMillis();

        while( iterSeq++ != iterCount ) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
                    pMessage.get("key"), pMessage.get("message"));
            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            if((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("####### IntervalCount:" + intervalCount +
                            " intervalMillis:" + intervalMillis + " #########");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if(interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis:" + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

        }
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        logger.info("{} millisecond elapsed for {} iterations", timeElapsed, iterCount);

    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage, boolean sync) {
        //  비동기 처리방식
        if(!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("async message:" + pMessage.get("key") + " partition:" + metadata.partition() +
                            " offset:" + metadata.offset());
                } else {
                    logger.error("exception error from broker " + exception.getMessage());
                }
            });

        } //동기 처리방식
        else {
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logger.info("sync message:" + pMessage.get("key") + " partition:" + metadata.partition() +
                        " offset:" + metadata.offset());
            } catch (ExecutionException e) {
                logger.error(e.getMessage());
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }

    }

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        //KafkaProducer configuration setting
        // null, "hello world"

        Properties props  = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "50000");

        // ACKS 옵션 Default = -1
        // ACKS = 0 일 경우 대기 없이 다음 바로 실행됨.
        // 동기처리되며, offset을 찾지 못해서, 기다리지 않고 그냥 보냄.
        // props.setProperty(ProducerConfig.ACKS_CONFIG, "0");


        //KafkaProducer object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        sendPizzaMessage( kafkaProducer   // 프로듀셔명
                        , topicName       // 토픽명
                        , -1              // -1이면 무한 루프로 생성.
                        , 1000            // 1초 간격으로 전송
                        , 0               // 매 건 수당 wait time
                        , 0               // 매 건 수당
                        , false           // Sync 구분 : true=Sync / false=ASync
                        );

        kafkaProducer.close();

    }
}