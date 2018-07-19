package com.alex.space.hadoop.example.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class MyPartition extends Partitioner<LongWritable, LongWritable> {

  @Override
  public int getPartition(LongWritable key, LongWritable value, int numTaskReduces) {
    if (key.get() == 2013) {
      return 0;
    } else if (key.get() == 2014) {
      return 1;
    } else {
      return 2;
    }
  }
}
