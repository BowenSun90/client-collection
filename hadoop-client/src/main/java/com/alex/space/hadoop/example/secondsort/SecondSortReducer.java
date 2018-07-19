package com.alex.space.hadoop.example.secondsort;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Second sort reducer
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class SecondSortReducer extends Reducer<CombinationKey, IntWritable, Text, Text> {

  private StringBuffer sb = new StringBuffer();
  private Text score = new Text();

  /**
   * 注意一下reduce的调用时机和次数： reduce每次处理一个分组的时候会调用一次reduce函数。 所谓的分组就是将相同的key对应的value放在一个集合中 例如：<sort1,1>
   * <sort1,2> 分组后的结果就是 <sort1,{1,2}>这个分组会调用一次reduce函数
   */
  @Override
  protected void reduce(CombinationKey key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    // 先清除上一个组的数据
    sb.delete(0, sb.length());

    for (IntWritable val : values) {
      sb.append(val.get() + ",");
    }
    if (sb.length() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }

    score.set(sb.toString());
    context.write(key.getFirstKey(), score);

    System.out.println("---------------------进入reduce()函数---------------------");
    System.out.println(
        "---------------------{[" + key.getFirstKey() + "," + key.getSecondKey() + "],[" + score
            + "]}");
    System.out.println("---------------------结束reduce()函数---------------------");
  }
}
