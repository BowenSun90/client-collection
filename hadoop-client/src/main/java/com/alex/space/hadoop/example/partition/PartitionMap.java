package com.alex.space.hadoop.example.partition;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 自定义分区
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class PartitionMap extends Mapper<Object, Text, LongWritable, LongWritable> {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] line = value.toString().split("\t");
    context.write(new LongWritable(Integer.parseInt(line[0])),
        new LongWritable(Integer.parseInt(line[1])));
  }
}