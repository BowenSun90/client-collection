package com.alex.space.hadoop.example.entity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * 自定义数据类型
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class EntityApp {

  public static void main(String[] args) {

    Configuration conf = new Configuration();
    try {
      int res = ToolRunner.run(conf, new EntityJob(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
