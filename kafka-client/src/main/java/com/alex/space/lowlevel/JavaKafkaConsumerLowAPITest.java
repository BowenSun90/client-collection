package com.alex.space.lowlevel;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Alex Created by Alex on 2018/8/23.
 */
@Slf4j
public class JavaKafkaConsumerLowAPITest {

  public static void main(String[] args) {

    JavaKafkaConsumerLowLevel example = new JavaKafkaConsumerLowLevel();
    long maxReads = 300;
    String topic = "test";
    int partitionID = 0;

    KafkaTopicPartitionInfo topicPartitionInfo = new KafkaTopicPartitionInfo(topic, partitionID);
    List<KafkaBrokerInfo> seeds = new ArrayList<>();
    seeds.add(new KafkaBrokerInfo("localhost", 9092));

    try {
      example.run(maxReads, topicPartitionInfo, seeds, "group1");
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 获取该topic所属的所有分区ID列表
    int timeout = 100000;
    int bufferSize = 64 * 1024;

    List partitionIds = example
        .fetchTopicPartitionIDs(seeds, topic, timeout, bufferSize, "client-id");

    partitionIds.forEach(x -> log.info("ID: " + x));
  }
}
