package com.alex.space.elastic.config;

import com.alex.space.common.config.BaseConfigLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * Elastic Config
 *
 * @author Alex Created by Alex on 2018/6/4.
 */
@Slf4j
public class ElasticConfig extends BaseConfigLoader {

  private final static String configPath = "/elastic/application.properties";

  private static ElasticConfig instance = null;

  public static synchronized ElasticConfig getInstance() {
    if (instance == null) {
      instance = new ElasticConfig();
    }
    return instance;
  }

  private ElasticConfig() {
    super(configPath);
  }


}
