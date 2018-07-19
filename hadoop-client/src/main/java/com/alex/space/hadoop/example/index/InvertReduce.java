package com.alex.space.hadoop.example.index;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 倒排索引
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class InvertReduce extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    String result = "";
    for (Text value : values) {
      result += value.toString() + " # ";
    }
    context.write(key, new Text(result));
  }
}