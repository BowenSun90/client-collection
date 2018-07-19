package com.alex.space.hadoop.example.comparator;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Comparator map
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class ComparatorMap extends Mapper<Object, Text, IntWritable, Text> {

  @Override
  public void map(Object key, Text value, Context context)
      throws NumberFormatException, IOException, InterruptedException {

    String[] split = value.toString().split("\t");
    context.write(new IntWritable(Integer.parseInt(split[1])), new Text(split[0]));
  }
}