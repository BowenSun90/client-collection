package com.alex.space.hadoop.example.simplesort;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * 简单排序
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
@Slf4j
public class SimpleSortApp {

  public static void main(String[] args) {

    log.info("SimpleSortJob start");
    Configuration conf = new Configuration();

    try {
      int res = ToolRunner.run(conf, new SimpleSortJob(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
