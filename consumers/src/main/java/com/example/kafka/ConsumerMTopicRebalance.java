package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


    /* *********************************************************************
        1. 토픽 생성하기
           토픽 삭제 : kafka-topics --bootstrap-server localhost:9092 --delete --topic topic-p3-t1
           토픽 생성 : kafka-topics --bootstrap-server localhost:9092 --create --topic topic-p3-t1 --partitions 3

           토픽 삭제 : kafka-topics --bootstrap-server localhost:9092 --delete --topic topic-p3-t2
           토픽 생성 : kafka-topics --bootstrap-server localhost:9092 --create --topic topic-p3-t2 --partitions 3

        2. 메시지 전송하기
           kafka-console-producer --bootstrap-server localhost:9092 --topic topic-p3-t1
           kafka-console-producer --bootstrap-server localhost:9092 --topic topic-p3-t2

        3. 전송완료 후 kafka topic 메시지 확인하기
           kafka-console-consumer --bootstrap-server localhost:9092 --group group-mtopic --topic topic-p3-t1 --property print.key=true --property print.value=true
           kafka-console-consumer --bootstrap-server localhost:9092 --group group-mtopic --topic topic-p3-t2 --property print.key=true --property print.value=true
             => 동일한 스크립트로 3개에 창으로 실행하면, 분산작업이 진행된다.

        4. 컨슈머 그룹 확인하기.
           kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group group-assign

        // default = "Range Assignor" : 기존 연결 모두 끊고, 재 연결 방식
            <Range 방식>
            GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
            group-assign    topic-p3-t1     0          3               3               0               consumer-group-assign-1-af5427a2-24b9-4784-b4ba-d8cf6e75a95b /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     1          2               2               0               consumer-group-assign-1-af5427a2-24b9-4784-b4ba-d8cf6e75a95b /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     2          1               1               0               consumer-group-assign-1-af5427a2-24b9-4784-b4ba-d8cf6e75a95b /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     0          4               4               0               consumer-group-assign-1-af5427a2-24b9-4784-b4ba-d8cf6e75a95b /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     1          2               2               0               consumer-group-assign-1-af5427a2-24b9-4784-b4ba-d8cf6e75a95b /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     2          0               0               0               consumer-group-assign-1-af5427a2-24b9-4784-b4ba-d8cf6e75a95b /192.168.56.1   consumer-group-assign-1
            ↓ 추가 기동
            GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
            group-assign    topic-p3-t1     0          3               3               0               consumer-group-assign-1-55bf4429-56b3-4526-9ebc-8ae29ea90a25 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t1     1          2               2               0               consumer-group-assign-1-55bf4429-56b3-4526-9ebc-8ae29ea90a25 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t1     2          1               1               0               consumer-group-assign-1-af5427a2-24b9-4784-b4ba-d8cf6e75a95b /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     0          4               4               0               consumer-group-assign-1-55bf4429-56b3-4526-9ebc-8ae29ea90a25 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t2     1          2               2               0               consumer-group-assign-1-55bf4429-56b3-4526-9ebc-8ae29ea90a25 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t2     2          0               0               0               consumer-group-assign-1-af5427a2-24b9-4784-b4ba-d8cf6e75a95b /192.168.56.1   consumer-group-assign-1

            ↓ 추가 기동
            GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
            group-assign    topic-p3-t1     0          3               3               0               consumer-group-assign-1-55bf4429-56b3-4526-9ebc-8ae29ea90a25 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t1     1          2               2               0               consumer-group-assign-1-af5427a2-24b9-4784-b4ba-d8cf6e75a95b /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     2          1               1               0               consumer-group-assign-1-c6b722b8-d3ec-4824-9978-a808189fc951 /192.168.56.1   consumer-group-assign-1  ← 2변경  Assigned
            group-assign    topic-p3-t2     0          4               4               0               consumer-group-assign-1-55bf4429-56b3-4526-9ebc-8ae29ea90a25 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t2     1          2               2               0               consumer-group-assign-1-af5427a2-24b9-4784-b4ba-d8cf6e75a95b /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     2          0               0               0               consumer-group-assign-1-c6b722b8-d3ec-4824-9978-a808189fc951 /192.168.56.1   consumer-group-assign-1  ← 2변경  Assigned


        // RoundRobin Assignor : 기존 연결 모두 끊고, 재 연결 방식
        //props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
            <Round Robin Assignor>
            GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
            group-assign    topic-p3-t1     0          3               3               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     1          2               2               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     2          1               1               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     0          4               4               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     1          2               2               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     2          0               0               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            ↓ 추가 기동
            GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
            group-assign    topic-p3-t1     0          3               3               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     1          2               2               0               consumer-group-assign-1-788e6786-1a1b-407d-bb4f-57d599380945 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t1     2          1               1               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     0          4               4               0               consumer-group-assign-1-788e6786-1a1b-407d-bb4f-57d599380945 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t2     1          2               2               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     2          0               0               0               consumer-group-assign-1-788e6786-1a1b-407d-bb4f-57d599380945 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            ↓ 추가 기동
            GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
            group-assign    topic-p3-t1     0          3               3               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     1          2               2               0               consumer-group-assign-1-47a98c72-da63-47c1-9082-841bfbc80950 /192.168.56.1   consumer-group-assign-1  ← 2변경  Assigned
            group-assign    topic-p3-t1     2          1               1               0               consumer-group-assign-1-788e6786-1a1b-407d-bb4f-57d599380945 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t2     0          4               4               0               consumer-group-assign-1-3a5a137e-f900-4faa-b916-7a6538d88f33 /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     1          2               2               0               consumer-group-assign-1-47a98c72-da63-47c1-9082-841bfbc80950 /192.168.56.1   consumer-group-assign-1  ← 2변경  Assigned
            group-assign    topic-p3-t2     2          0               0               0               consumer-group-assign-1-788e6786-1a1b-407d-bb4f-57d599380945 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned


        // Cooperative Sticky Assignor : 기존 연결 유지하고, 재 연결 방식
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
            <Cooperative Sticky Assignor>
            GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
            group-assign    topic-p3-t1     0          3               3               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     1          2               2               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     2          1               1               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     0          4               4               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     1          2               2               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     2          0               0               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            ↓ 추가 기동
            GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
            group-assign    topic-p3-t1     0          3               3               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     1          2               2               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     2          1               1               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     0          4               4               0               consumer-group-assign-1-9886bb91-fd29-41c8-8a70-8367fe4f7896 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t2     1          2               2               0               consumer-group-assign-1-9886bb91-fd29-41c8-8a70-8367fe4f7896 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t2     2          0               0               0               consumer-group-assign-1-9886bb91-fd29-41c8-8a70-8367fe4f7896 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            ↓ 추가 기동
            GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
            group-assign    topic-p3-t1     0          3               3               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     1          2               2               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     2          1               1               0               consumer-group-assign-1-b05ae055-63c6-4607-8626-51e266b8906d /192.168.56.1   consumer-group-assign-1  ← 2변경  Assigned
            group-assign    topic-p3-t2     0          4               4               0               consumer-group-assign-1-9886bb91-fd29-41c8-8a70-8367fe4f7896 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t2     1          2               2               0               consumer-group-assign-1-9886bb91-fd29-41c8-8a70-8367fe4f7896 /192.168.56.1   consumer-group-assign-1  ← 1변경  Assigned
            group-assign    topic-p3-t2     2          0               0               0               consumer-group-assign-1-b05ae055-63c6-4607-8626-51e266b8906d /192.168.56.1   consumer-group-assign-1  ← 2변경  Assigned
            ↓ Stop 기동
            GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
            group-assign    topic-p3-t1     0          3               3               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     1          2               2               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t1     2          1               1               0               consumer-group-assign-1-b05ae055-63c6-4607-8626-51e266b8906d /192.168.56.1   consumer-group-assign-1  ← 2변경  Assigned
            group-assign    topic-p3-t2     0          4               4               0               consumer-group-assign-1-956d9d44-ea9a-4f5d-869d-6b7f25cf2b7a /192.168.56.1   consumer-group-assign-1
            group-assign    topic-p3-t2     1          2               2               0               consumer-group-assign-1-b05ae055-63c6-4607-8626-51e266b8906d /192.168.56.1   consumer-group-assign-1  ← 2변경  Assigned  ← 1변경  Assigned
            group-assign    topic-p3-t2     2          0               0               0               consumer-group-assign-1-b05ae055-63c6-4607-8626-51e266b8906d /192.168.56.1   consumer-group-assign-1  ← 2변경  Assigned

    * */

public class ConsumerMTopicRebalance {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerMTopicRebalance.class.getName());

    public static void main(String[] args) {

        //String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-mtopic");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");
//        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01-static");
//        props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "3");

        // default = "Range Assignor"
        // RoundRobin Assignor
        //props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        // Cooperative Sticky Assignor
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

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
                    logger.info("topic:{}, record key:{},  partition:{}, record offset:{} record value:{}",
                            record.topic(), record.key(), record.partition(), record.offset(), record.value());
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
