package com.alex.space.hadoop.example.comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Comparator application
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class ComparatorApp {

  public static void main(String[] args) {

    Configuration conf = new Configuration();
    try {
      int res = ToolRunner.run(conf, new ComparatorJob(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
