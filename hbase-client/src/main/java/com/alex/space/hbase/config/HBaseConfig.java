package com.alex.space.hbase.config;

import com.alex.space.common.config.BaseConfigLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * HBase Config
 *
 * @author Alex Created by Alex on 2018/6/4.
 */
@Slf4j
public class HBaseConfig extends BaseConfigLoader {

  private static final String configPath = "/hbase/application.properties";

  private static HBaseConfig instance = null;

  public static synchronized HBaseConfig getInstance() {
    if (instance == null) {
      instance = new HBaseConfig();
    }
    return instance;
  }

  private HBaseConfig() {
    super(configPath);
  }

}
