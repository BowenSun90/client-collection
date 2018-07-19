package com.alex.space.hadoop.example.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 单词计数
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] spilted = value.toString().split(" ");
    for (String word : spilted) {
      context.write(new Text(word), new IntWritable(1));
    }
  }
}
