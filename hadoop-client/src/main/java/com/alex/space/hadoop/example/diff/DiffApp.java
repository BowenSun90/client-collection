package com.alex.space.hadoop.example.diff;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * 数据去重
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DiffApp {

  public static void main(String[] args) {

    Configuration conf = new Configuration();
    try {
      int res = ToolRunner.run(conf, new DiffJob(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
