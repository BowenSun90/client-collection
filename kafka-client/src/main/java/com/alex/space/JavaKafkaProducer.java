package com.alex.space;

import com.alex.space.common.utils.StringUtils;
import com.alex.space.config.KafkaConfig;
import com.alex.space.config.KafkaConstants;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka 生产者
 *
 * 通过Kafka提供的API进行数据产生操作的测试类
 *
 * @author Alex Created by Alex on 2018/8/22.
 */
@Slf4j
public class JavaKafkaProducer {

  private static KafkaConfig kafkaConfig = KafkaConfig.getInstance();

  public static void main(String[] args) {
    String brokerList = kafkaConfig.getProperty(KafkaConstants.BROKER_LIEST);
    String topic = kafkaConfig.getProperty(KafkaConstants.DEFAULT_TOPIC);
    log.info("broker list: " + brokerList);
    log.info("topic: " + topic);

    // 1. 构建kafka properties
    Properties props = buildKafkaProperties(brokerList);

    // 2. 构建Kafka Producer Configuration上下文
    ProducerConfig config = new ProducerConfig(props);

    // 3. 构建Producer对象
    final Producer<String, String> producer = new Producer<>(config);

    // 4.1 顺序发消息
    for (int i = 0; i < 10; i++) {
      KeyedMessage message = generateKeyedMessage(topic);
      producer.send(message);
      log.info("Send: " + message);
    }

    final AtomicBoolean flag = new AtomicBoolean(true);
    // 4.2 多线程发消息
    int numThreads = 5;
    ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < 10; i++) {
      pool.submit(new Thread(
          () -> {
            while (flag.get()) {
              // Send message
              KeyedMessage message = generateKeyedMessage(topic);
              producer.send(message);
              log.info("Send: " + message);

              try {
                Thread.sleep(5000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }

            log.info(Thread.currentThread().getName() + " shutdown....");
          }
      ), "Thread-" + i);
    }

    // 5. 等待执行完成，控制暂停消费
    try {
      Thread.sleep(60000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    flag.set(false);

    // 6. 关闭资源
    pool.shutdown();
    try {
      pool.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      producer.close();
    }


  }

  private static Properties buildKafkaProperties(final String brokerList) {
    Properties props = new Properties();
    props.put("metadata.broker.list", brokerList);
    /*
     * 0表示不等待结果返回
     * 1表示等待至少有一个服务器返回数据接收标识
     * -1表示必须接收到所有的服务器返回标识，及同步写入
     * */
    props.put("request.required.acks",
        kafkaConfig.getProperty("request.required.acks", "0"));
    /*
     * 内部发送数据是异步还是同步
     * sync：同步, 默认
     * async：异步
     */
    props.put("producer.type",
        kafkaConfig.getProperty("producer.type", "async"));
    /*
     * 设置序列化的类
     * 可选：kafka.serializer.StringEncoder
     * 默认：kafka.serializer.DefaultEncoder
     */
    props.put("serializer.class",
        kafkaConfig.getProperty("serializer.class", "kafka.serializer.StringEncoder"));
    /*
     * 设置分区类
     * 根据key进行数据分区
     * 默认是：kafka.producer.DefaultPartitioner ==> 安装key的hash进行分区
     * 可选:kafka.serializer.ByteArrayPartitioner ==> 转换为字节数组后进行hash分区
     */
    props.put("partitioner.class",
        "com.alex.space.JavaKafkaProducerPartitioner");

    // 重试次数
    props.put("message.send.max.retries",
        kafkaConfig.getProperty("message.send.max.retries", "3"));

    // 异步提交的时候(async)，并发提交的记录数
    props.put("batch.num.messages",
        kafkaConfig.getProperty("batch.num.messages", "10"));

    // 设置缓冲区大小，默认10KB
    props.put("send.buffer.bytes",
        kafkaConfig.getProperty("send.buffer.bytes", "102400"));

    return props;
  }

  /**
   * 产生一个消息
   */
  private static KeyedMessage<String, String> generateKeyedMessage(String topic) {
    String key = "key_" + ThreadLocalRandom.current().nextInt(10, 99);
    StringBuilder sb = new StringBuilder();
    int num = ThreadLocalRandom.current().nextInt(1, 5);
    for (int i = 0; i < num; i++) {
      sb.append(StringUtils.generateStringMessage(ThreadLocalRandom.current().nextInt(3, 20)))
          .append(" ");
    }
    String message = sb.toString().trim();
    return new KeyedMessage(topic, key, message);
  }


}
