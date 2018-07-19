package com.alex.space.hadoop.example.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 倒排索引
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class InvertMap extends Mapper<Object, Text, Text, Text> {

  @Override
  public void map(Object key, Text value, Context context) {
    String line = value.toString();
    try {
      String[] lineSplit = line.split("\t");
      String newKey = lineSplit[0];
      String newValue = lineSplit[1];
      context.write(new Text(newValue), new Text(newKey));
    } catch (Exception e) {
      context.getCounter(Counter.LINESKIP).increment(1);
      return;
    }
  }

  enum Counter {
    /**
     * 出错的行
     */
    LINESKIP
  }

}