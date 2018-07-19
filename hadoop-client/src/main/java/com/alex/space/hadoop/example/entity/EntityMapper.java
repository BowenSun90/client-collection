package com.alex.space.hadoop.example.entity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * entity job
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class EntityMapper extends Mapper<Object, Text, Text, MyEntity> {

  @Override
  public void map(Object key, Text value, Context context) {
    try {
      String[] split = value.toString().split("\t");
      String keyNum = split[1];
      MyEntity ws = new MyEntity(split[6], split[7], split[8], split[9]);
      System.out.println(split[6] + "| " + split[7] + "|" + split[8] + "|" + split[9]);
      context.write(new Text(keyNum), ws);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}