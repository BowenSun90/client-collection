package com.alex.space.config;

import com.alex.space.common.config.BaseConfigLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka Config
 *
 * @author Alex Created by Alex on 2018/6/4.
 */
@Slf4j
public class KafkaConfig extends BaseConfigLoader {

  private final static String configPath = "/kafka/application.properties";

  private static KafkaConfig instance = null;

  public static synchronized KafkaConfig getInstance() {
    if (instance == null) {
      instance = new KafkaConfig();
    }
    return instance;
  }

  private KafkaConfig() {
    super(configPath);
  }


}
