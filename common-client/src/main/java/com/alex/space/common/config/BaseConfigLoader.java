package com.alex.space.common.config;

import com.alex.space.common.CommonConstants;
import com.alex.space.common.utils.StringUtils;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Config loader
 *
 * @author Alex Created by Alex on 2018/6/6.
 */
@Slf4j
public abstract class BaseConfigLoader {

  private Properties prop = new Properties();

  protected BaseConfigLoader(String configPath) {

    try {
      String nfsConfig = CommonConstants.NFS_ROOT + configPath;
      // support external config file
      if (new File(nfsConfig).exists()) {
        log.info("Config file path: " + nfsConfig);
        prop.load(new FileInputStream(nfsConfig));
      } else {
        prop.load(this.getClass().getClassLoader().getResourceAsStream("application.properties"));
      }

    } catch (Exception e) {
      log.error("Load config file with exception", e);
    }
  }

  public String getProperty(String key) {
    return prop.getProperty(key);
  }

  public String getProperty(String key, String defaultValue) {
    String value = getProperty(key);
    return StringUtils.isEmpty(value) ? defaultValue : value;
  }

  public Integer getIntProperty(String key) {
    return Integer.parseInt(prop.getProperty(key));
  }

}
