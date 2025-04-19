package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
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
 *   - offset 확인하기.
 *      kafka-console-consumer --consumer.config /home/anakin/consumer_temp.config  --bootstrap-server localhost:9092 --topic __consumer_offsets  --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" | grep simple-topic
 * */

public class ConsumerWakeup {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerWakeup.class.getName());

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01");
        //props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01-static");
        props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1");
        //props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "2");
        //props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "3");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(List.of(topicName));

        //main thread
        Thread mainThread = Thread.currentThread();

        //main thread 종료시 별도의 thread로 KafkaConsumer wakeup()메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info(" main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch(InterruptedException e) { e.printStackTrace();}
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{},  partition:{}, record offset:{} record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }
            }
        }catch(WakeupException e) {
            logger.error("wakeup exception has been called");
        }finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }

        //kafkaConsumer.close();

    }
}
