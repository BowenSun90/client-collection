package com.alex.space.hadoop.example.diff;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Diff job
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DiffReduce extends Reducer<Text, NullWritable, Text, NullWritable> {

  @Override
  public void reduce(Text key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }
}