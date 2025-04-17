package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;



/* **
 *  <kafka 토픽 생성 및 확인>
 *   - 토픽 생성
 *      kafka-topics --bootstrap-server localhost:9092 --delete --topic pizza-topic
 *      kafka-topics --bootstrap-server localhost:9092 --create --topic pizza-topic --partitions 3
 *   - 토픽 확인
 *      kafka-topics --bootstrap-server localhost:9092 --describe --topic pizza-topic
           : Topic: pizza-topic      TopicId: o6hyBGyITkazYGQskdvuUw PartitionCount: 3       ReplicationFactor: 1    Configs: segment.bytes=1073741824
                Topic: pizza-topic      Partition: 0    Leader: 0       Replicas: 0     Isr: 0
                Topic: pizza-topic      Partition: 1    Leader: 0       Replicas: 0     Isr: 0
                Topic: pizza-topic      Partition: 2    Leader: 0       Replicas: 0     Isr: 0

 *   - 메시지전송 테스트
 *      kafka-console-producer --bootstrap-server localhost:9092  --topic pizza-topic
 *   - 메시지수신 테스트
 *      kafka-console-consumer --bootstrap-server localhost:9092 --topic pizza-topic
 *      kafka-console-consumer --bootstrap-server localhost:9092 --topic pizza-topic --from-beginning
 *   - offset 확인하기.
 *      kafka-console-consumer --consumer.config /home/anakin/consumer_temp.config  --bootstrap-server localhost:9092 --topic __consumer_offsets  --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" | grep simple-topic
 * */


public class ConsumerPartitionAssignSeek {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerPartitionAssignSeek.class.getName());

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assign_seek_v001");
        //props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        //kafkaConsumer.subscribe(List.of(topicName));
        
        // 옵셋 지정하여 읽어 오는 옵션
        kafkaConsumer.assign(Arrays.asList(topicPartition));
        kafkaConsumer.seek(topicPartition, 30L);

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

        //kafkaConsumer.close();
        //pollAutoCommit(kafkaConsumer);
        //pollCommitSync(kafkaConsumer);
        //pollCommitAsync(kafkaConsumer);
        pollNoCommit(kafkaConsumer);

    }

    private static void pollNoCommit(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ######## loopCnt: {} consumerRecords count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{},  partition:{}, record offset:{} record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }
            }
        }catch(WakeupException e) {
            logger.error("wakeup exception has been called");
        }catch(Exception e) {
            logger.error(e.getMessage());
        }finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

    private static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ######## loopCnt: {} consumerRecords count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{},  partition:{}, record offset:{} record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }
                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                      if(exception != null) {
                          logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                      }
                    }
                });

            }
        }catch(WakeupException e) {
            logger.error("wakeup exception has been called");
        }catch(Exception e) {
            logger.error(e.getMessage());
        }finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }

    }

    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ######## loopCnt: {} consumerRecords count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{},  partition:{}, record offset:{} record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }
                try {
                    if(consumerRecords.count() > 0 ) {
                        kafkaConsumer.commitSync();
                        logger.info("commit sync has been called");
                    }
                } catch(CommitFailedException e) {
                    logger.error(e.getMessage());
                }

            }
        }catch(WakeupException e) {
            logger.error("wakeup exception has been called");
        }catch(Exception e) {
            logger.error(e.getMessage());
        }finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }

    }


    public static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ######## loopCnt: {} consumerRecords count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{},  partition:{}, record offset:{} record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }
                try {
                    logger.info("main thread is sleeping {} ms during while loop", 10000);
                    Thread.sleep(10000);
                }catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }catch(WakeupException e) {            logger.error("wakeup exception has been called");
        }finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }

    }
}
