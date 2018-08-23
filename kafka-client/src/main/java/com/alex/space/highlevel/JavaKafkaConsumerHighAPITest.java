package com.alex.space.highlevel;

import com.alex.space.config.KafkaConfig;

/**
 * @author Alex Created by Alex on 2018/8/22.
 */
public class JavaKafkaConsumerHighAPITest {

  public static void main(String[] args) {
    String zookeeper = KafkaConfig.getInstance().getProperty("zk.host", "localhost:2181");
    String groupId = KafkaConfig.getInstance().getProperty("group.id", "");
    String topic = KafkaConfig.getInstance().getProperty("topic", "test");

    int threads = 1;
    JavaKafkaConsumerHighLevel example =
        new JavaKafkaConsumerHighLevel(topic, threads, zookeeper, groupId);

    Thread t = new Thread(example);
    t.start();

    // 执行10秒后结束
    int sleepMillis = 600000;
    try {
      Thread.sleep(sleepMillis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // 关闭
    example.shutdown();
  }
}
