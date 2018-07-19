package com.alex.space.hadoop.example.join;

import java.io.IOException;
import java.util.Vector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce-side join
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

  /**
   * 用来存放来自tb_a表的数据
   */
  Vector<String> vectorA = new Vector<String>();
  /**
   * 用来存放来自tb_b表的
   */
  Vector<String> vectorB = new Vector<String>();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    vectorA.clear();
    vectorB.clear();

    // 迭代集合数据
    for (Text val : values) {
      // 将集合中的数据对应添加到Vector中
      if (val.toString().startsWith("a#")) {
        vectorA.add(val.toString().substring(2));
      } else if (val.toString().startsWith("b#")) {
        vectorB.add(val.toString().substring(2));
      }
    }

    // 获取两个Vector集合的长度
    int sizeA = vectorA.size();
    int sizeB = vectorB.size();

    // 遍历两个向量将结果写出去
    for (int i = 0; i < sizeA; i++) {
      for (int j = 0; j < sizeB; j++) {
        context.write(key, new Text("   " + vectorA.get(i) + "  " + vectorB.get(j)));
      }
    }

  }
}
