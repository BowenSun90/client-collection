package com.alex.space.hadoop.example.join;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Reduce-side join
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 获取输入文件的全路径和名称
    FileSplit fileSplit = (FileSplit) context.getInputSplit();
    String path = fileSplit.getPath().toString();

    // 获取输入记录的字符串
    String line = value.toString();
    if (line == null || line.equals("")) {
      return;
    }

    // 处理来自tb_a表的记录
    if (path.contains("tb_a")) {
      // 按制表符切割
      String[] values = line.split("\t");
      // 当数组长度小于2时，视为无效记录
      if (values.length < 2) {
        return;
      }
      // 获取id和name
      String id = values[0];
      String name = values[1];

      // 把结果写出去
      context.write(new Text(id), new Text("a#" + name));
    } else if (path.contains("tb_b")) {
      // 按制表符切割
      String[] values = line.split("\t");
      // 当长度不为3时，视为无效记录
      if (values.length < 3) {
        return;
      }

      // 获取属性
      String id = values[0];
      String statyear = values[1];
      String num = values[2];

      // 写出去
      context.write(new Text(id), new Text("b#" + statyear + "  " + num));
    }

  }
}
