package com.alex.space.zoo.config;

import com.alex.space.common.config.BaseConfigLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * Zookeeper Config
 *
 * @author Alex Created by Alex on 2018/6/4.
 */
@Slf4j
public class ZooConfig extends BaseConfigLoader {

  private final static String configPath = "/zoo/application.properties";

  private static ZooConfig instance = null;

  public static synchronized ZooConfig getInstance() {
    if (instance == null) {
      instance = new ZooConfig();
    }
    return instance;
  }

  private ZooConfig() {
    super(configPath);
  }

}
