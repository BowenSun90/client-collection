package com.alex.space.hadoop.example.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * 二次排序
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class SecondSortApp {

  public static void main(String[] args) {

    Configuration conf = new Configuration();
    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
    try {
      int res = ToolRunner.run(conf, new SecondSortJob(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
