package com.alex.space.hadoop.example.simplesort;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Simple sort reduce class
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class SimpleSortReduce extends Reducer<MyKey, LongWritable, LongWritable, LongWritable> {

  @Override
  public void reduce(MyKey key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {

    context.write(new LongWritable(key.myk2), new LongWritable(key.myv2));
  }
}