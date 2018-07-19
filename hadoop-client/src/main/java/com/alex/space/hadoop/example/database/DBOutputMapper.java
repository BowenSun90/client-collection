package com.alex.space.hadoop.example.database;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Database output application
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DBOutputMapper extends Mapper<LongWritable, Student, LongWritable, User> {

  private User user = new User();

  @Override
  protected void map(LongWritable key, Student value, Context context)
      throws IOException, InterruptedException {

    // 封装user对象
    user.setId(value.getId());
    user.setName(value.getName());

    // 把user对象作为value写出去
    context.write(key, user);
  }
}
