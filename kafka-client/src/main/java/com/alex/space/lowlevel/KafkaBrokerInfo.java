package com.alex.space.lowlevel;

/**
 * Kafka服务器连接参数
 *
 * @author Alex Created by Alex on 2018/8/22.
 */
public class KafkaBrokerInfo {

  /**
   * 主机名
   */
  public final String brokerHost;

  /**
   * 端口号
   */
  public final int brokerPort;

  private final static int DEFAULT_PORT = 9092;

  /**
   * 构造方法
   *
   * @param brokerHost Kafka服务器主机或者IP地址
   * @param brokerPort 端口号
   */
  KafkaBrokerInfo(String brokerHost, int brokerPort) {
    this.brokerHost = brokerHost;
    this.brokerPort = brokerPort;
  }

  /**
   * 构造方法， 使用默认端口号9092进行构造
   *
   * @param brokerHost Kafka服务器主机或者IP地址
   */
  public KafkaBrokerInfo(String brokerHost) {
    this(brokerHost, DEFAULT_PORT);
  }

}
