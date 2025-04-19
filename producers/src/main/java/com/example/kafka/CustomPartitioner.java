package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {
    public static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class.getName());
    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();
    private String specialKeyName;

    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("custom.specialKey").toString();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 전체 파티션을 가져온다.
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        // 전체 파티션 사이즈를 구한다.
        int numPartitions = partitionInfoList.size();

        // 전체 중 2개의 파티션 할당.
        int numSpecialPartitions = (int)(numPartitions * 0.5);
        int partitionIndex = 0;

        if (keyBytes == null) {
            //return stickyPartitionCache.partition(topic, cluster);
            throw new InvalidRecordException("key should not be null");
        }

        // "specialKey=P001"인 경우 0과 1 파티션에 할당하기.
        if (((String)key).equals(specialKeyName)) {    // {0,1}
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        }
        // 나머지 파티션에 할당하기.
        else {                                         // {2,3,4} = {0,1,2,3,4} - {0,1}
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }
        logger.info("key:{} is sent to partition:{}", key.toString(), partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }


}
