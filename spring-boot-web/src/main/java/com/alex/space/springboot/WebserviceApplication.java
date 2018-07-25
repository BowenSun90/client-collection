package com.alex.space.springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Application
 *
 * @author Alex Created by Alex on 2017/07/14.
 */
@EnableSwagger2
@SpringBootApplication
public class WebserviceApplication extends SpringBootServletInitializer {

  public static void main(String[] args) {
    SpringApplication.run(WebserviceApplication.class, args);
  }

}
