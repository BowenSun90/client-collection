package com.alex.space.hadoop.example.database;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Database input
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DBInputReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

  @Override
  protected void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    for (Text t : values) {
      context.write(key, t);
    }
  }
}