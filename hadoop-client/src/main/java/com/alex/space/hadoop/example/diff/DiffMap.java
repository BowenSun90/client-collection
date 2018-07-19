package com.alex.space.hadoop.example.diff;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Diff job
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DiffMap extends Mapper<Object, Text, Text, NullWritable> {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    context.write(value, NullWritable.get());
  }
}