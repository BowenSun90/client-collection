package com.alex.space.config;

/**
 * @author Alex Created by Alex on 2018/8/22.
 */
public interface KafkaConstants {

  String BROKER_LIST = "broker";

  String DEFAULT_TOPIC = "topic";

  int SO_TIMEOUT = 100000;

  int BUFFER_SIZE = 64 * 1024;

  /**
   * 最大重试次数
   */
  int MAX_RETRY_TIMES = 5;

  /**
   * 最小重试次数
   */
  int MIN_RETRY_TIMES = 3;

  /**
   * 重试间隔时间
   */
  long RETRY_INTERVAK_MILLIS = 1000L;
}
