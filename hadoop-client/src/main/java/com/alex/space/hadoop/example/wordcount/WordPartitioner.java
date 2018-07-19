package com.alex.space.hadoop.example.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 单词计数
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class WordPartitioner extends Partitioner<Text, IntWritable> {

  @Override
  public int getPartition(Text key, IntWritable value, int numTaskReduces) {
    if (key.toString().compareTo("Hello") == 0) {
      return 0;
    } else {
      return 1;
    }
  }
}

