package com.alex.space.lowlevel;

/**
 * 读取具体分区的信息
 *
 * @author Alex Created by Alex on 2018/8/22.
 */
public class KafkaTopicPartitionInfo {

  /**
   * 主题名称
   */
  public final String topic;
  /**
   * 分区id
   */
  public final int partitionID;

  /**
   * 构造函数
   *
   * @param topic 主题名称
   * @param partitionID 分区id
   */
  KafkaTopicPartitionInfo(String topic, int partitionID) {
    this.topic = topic;
    this.partitionID = partitionID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KafkaTopicPartitionInfo that = (KafkaTopicPartitionInfo) o;

    boolean result = false;
    if (partitionID == that.partitionID) {
      result = topic != null ? topic.equals(that.topic) : that.topic == null;
    }

    return result;
  }

  @Override
  public int hashCode() {
    int result = topic != null ? topic.hashCode() : 0;
    result = 31 * result + partitionID;
    return result;
  }


}
