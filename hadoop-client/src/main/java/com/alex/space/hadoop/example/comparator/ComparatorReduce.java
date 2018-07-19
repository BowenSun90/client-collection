package com.alex.space.hadoop.example.comparator;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Comparator reduce
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class ComparatorReduce extends Reducer<IntWritable, Text, Text, IntWritable> {

  @Override
  public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    for (Text text : values) {
      context.write(text, key);
    }
  }
}