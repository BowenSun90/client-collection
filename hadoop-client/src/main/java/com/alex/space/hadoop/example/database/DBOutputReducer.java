package com.alex.space.hadoop.example.database;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Database output
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DBOutputReducer extends Reducer<LongWritable, User, User, Text> {

  @Override
  protected void reduce(LongWritable key, Iterable<User> values,
      Reducer<LongWritable, User, User, Text>.Context context)
      throws IOException, InterruptedException {
    for (User user : values) {
      context.write(user, new Text(user.getName()));
    }
  }

}
