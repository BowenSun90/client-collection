package com.alex.space.highlevel;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Alex Created by Alex on 2018/8/22.
 */
@Slf4j
public class ConsumerKafkaStreamProcesser implements Runnable {

  /**
   * Kafka数据流
   */
  private KafkaStream<String, String> stream;

  /**
   * 线程ID编号
   */
  private int threadNumber;

  public ConsumerKafkaStreamProcesser(KafkaStream<String, String> stream, int threadNumber) {
    this.stream = stream;
    this.threadNumber = threadNumber;
  }

  @Override
  public void run() {
    // 1. 获取数据迭代器
    ConsumerIterator<String, String> iter = this.stream.iterator();

    // 2. 迭代输出数据
    while (iter.hasNext()) {
      // 2.1 获取数据值
      MessageAndMetadata value = iter.next();

      // 2.2 输出
      log.info(
          this.threadNumber + ":" + ":" + value.offset() + value.key() + ":" + value.message());
    }
    // 3. 表示当前线程执行完成
    log.info("Shutdown Thread:" + this.threadNumber);
  }
}
