package com.alex.space.hadoop.example.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 单词计数
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws java.io.IOException, InterruptedException {
    int count = 0;
    for (IntWritable value : values) {
      count += value.get();
    }
    context.write(key, new IntWritable(count));
  }
}
