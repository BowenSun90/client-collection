package com.alex.space;


import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 自定义的数据分区器
 *
 * 决定输入的key/value键值对的message发送到Topic的那个分区中，返回分区id，范围:[0,分区数量)
 *
 * @author Alex Created by Alex on 2018/8/22.
 */
public class JavaKafkaProducerPartitioner implements Partitioner {

  /**
   * 构造函数，必须给定
   *
   * @param properties 上下文
   */
  public JavaKafkaProducerPartitioner(VerifiableProperties properties) {
    // nothings
  }

  /**
   * 无参构造函数
   */
  public JavaKafkaProducerPartitioner() {
    this(new VerifiableProperties());
  }

  @Override
  public int partition(Object key, int numPartitions) {
    int h;
    int newHashCode = (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    int partitionNum = Math.abs(newHashCode) % numPartitions;
    return partitionNum;
  }
}
