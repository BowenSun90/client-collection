package com.alex.space.springboot.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Application
 *
 * @author Alex Created by Alex on 2017/06/01
 */
@Configuration
@ConfigurationProperties(prefix = "guava.cache")
@Data
public class GuavaCacheProperties {

  private long maximumSize;

  private long duration;

  private String unit;
}
