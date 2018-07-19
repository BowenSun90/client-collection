package com.alex.space.hadoop.example.partition;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 自定义分区
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class PartitionReduce extends
    Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

  @Override
  public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    for (LongWritable longWritable : values) {
      context.write(key, longWritable);
    }
  }
}