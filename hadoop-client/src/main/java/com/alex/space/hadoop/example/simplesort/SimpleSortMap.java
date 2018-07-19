package com.alex.space.hadoop.example.simplesort;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Simple sort map class
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class SimpleSortMap extends Mapper<Object, Text, MyKey, LongWritable> {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] split = line.split("\t");
    MyKey my = new MyKey(Long.parseLong(split[0]), Long.parseLong(split[1]));
    context.write(my, new LongWritable(1));
  }

}