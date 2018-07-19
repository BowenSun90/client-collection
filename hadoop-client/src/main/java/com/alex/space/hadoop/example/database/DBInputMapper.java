package com.alex.space.hadoop.example.database;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Database input
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DBInputMapper extends Mapper<LongWritable, Student, LongWritable, Text> {

  @Override
  protected void map(LongWritable key, Student value, Context context)
      throws IOException, InterruptedException {

    context.write(new LongWritable(value.getId()), new Text(value.getName()));
  }
}
