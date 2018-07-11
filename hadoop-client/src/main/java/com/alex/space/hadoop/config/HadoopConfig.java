package com.alex.space.hadoop.config;

import com.alex.space.common.config.BaseConfigLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Alex Created by Alex on 2018/6/6.
 */
@Slf4j
public class HadoopConfig extends BaseConfigLoader {

  private static final String configPath = "/hadoop/application.properties";

  private static HadoopConfig instance = null;

  public static synchronized HadoopConfig getInstance() {
    if (instance == null) {
      instance = new HadoopConfig();
    }
    return instance;
  }

  private HadoopConfig() {
    super(configPath);
  }

}
