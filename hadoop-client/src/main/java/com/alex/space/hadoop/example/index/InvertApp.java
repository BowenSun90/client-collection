package com.alex.space.hadoop.example.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * 倒排索引
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class InvertApp {

  public static void main(String[] args) {

    Configuration conf = new Configuration();
    try {
      int res = ToolRunner.run(conf, new InvertJob(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
