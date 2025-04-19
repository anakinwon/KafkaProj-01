package com.practice.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/* **
 *  <kafka 토픽 생성 및 확인>
 *   - 토픽 생성
 *      kafka-topics --bootstrap-server localhost:9092 --delete --topic file-topic
 *      kafka-topics --bootstrap-server localhost:9092 --create --topic file-topic --partitions 3
 *   - 토픽 확인
 *      kafka-topics --bootstrap-server localhost:9092 --describe --topic file-topic
 *         : Topic: file-topic       TopicId: 4Qcej8StRQyq7vfxfFOtdg PartitionCount: 3       ReplicationFactor: 1    Configs: segment.bytes=1073741824
 *              Topic: file-topic       Partition: 0    Leader: 0       Replicas: 0     Isr: 0
 *              Topic: file-topic       Partition: 1    Leader: 0       Replicas: 0     Isr: 0
 *              Topic: file-topic       Partition: 2    Leader: 0       Replicas: 0     Isr: 0
 *
 *   - Consumer 확인
 *      kafka-console-consumer --bootstrap-server localhost:9092 --group group-file --topic file-topic --property print.key=true --property print.value=true --from-beginning
 *
 * */


public class FileProducer {
    public static final Logger logger = LoggerFactory.getLogger(FileProducer.class.getName());
    public static void main(String[] args) {

        String topicName = "file-topic";

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
        String filePath = "D:\\kafka-Core\\practice\\src\\main\\resources\\pizza_sample.txt";

        //KafkaProducer객체 생성->ProducerRecords생성 -> send() 비동기 방식 전송
        sendFileMessages(kafkaProducer, topicName, filePath);
        kafkaProducer.close();

    }

    // 파일처리 메소드
    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String filePath) {
        String line = "";
        final String delimiter = ","; // 첫번째 컴마 분리해서 Key생성

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while( (line = bufferedReader.readLine()) != null) {
                // 첫번째 컴마 분리해서 Key로 생성
                String[] tokens = line.split(delimiter);
                String key = tokens[0];

                // 나머지는 모두 붙여서 Value로 생성 <- 1,000번 루핑...
                StringBuffer value = new StringBuffer();
                for(int i=1; i<tokens.length; i++) {
                    // 라인의 마지막에는 컴마를 삭제
                    if( i != (tokens.length -1)) {
                        value.append(tokens[i] + ",");
                    } else {
                        value.append(tokens[i]);
                    }
                }

                // 생성된 토큰을 메시지로 보내기.
                //"ord0, P001, Cheese Pizza, Erick Koelpin, (235) 592-3785 x9190, 6373 Gulgowski Path, 2022-07-14 12:09:33"
                sendMessage(kafkaProducer, topicName, key, value.toString());
            }

        }catch(IOException e) {
            logger.info(e.getMessage());
        }
    }

    //"ord0, P001, Cheese Pizza, Erick Koelpin, (235) 592-3785 x9190, 6373 Gulgowski Path, 2022-07-14 12:09:33"
    private static void sendMessage( KafkaProducer<String, String> kafkaProducer
                                   , String topicName
                                   , String key
                                   , String value) {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        logger.info("key:{}, value:{}", key, value);

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

}