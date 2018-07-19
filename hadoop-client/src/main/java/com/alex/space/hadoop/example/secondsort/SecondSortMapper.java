package com.alex.space.hadoop.example.secondsort;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Second sort mapper
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class SecondSortMapper extends Mapper<Text, Text, CombinationKey, IntWritable> {

  /**
   * 为什么要将这些变量写在map函数外边
   *
   * 对于分布式的程序，一定要注意到内存的使用情况，对于MapReduce框架 每一行的原始记录的处理都要调用一次map()函数，假设，这个map()函数要处理1一亿
   * 条输入记录，如果将这些变量都定义在map函数里面则会导致这4个变量的对象句柄 非常的多(极端情况下将产生4*1亿个句柄，当然java也是有自动的GC机制的，一定不会达到这么多)
   * 导致栈内存被浪费掉，我们将其写在map函数外面，顶多就只有4个对象句柄
   */
  private CombinationKey combinationKey = new CombinationKey();
  private Text sortName = new Text();
  private IntWritable score = new IntWritable();

  @Override
  protected void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
    System.out.println("---------------------进入map()函数---------------------");

    // 过滤非法记录(这里用计数器比较好)
    if (key == null || value == null || key.toString().equals("")) {
      return;
    }

    sortName.set(key.toString());
    score.set(Integer.parseInt(value.toString().trim()));
    combinationKey.setFirstKey(sortName);
    combinationKey.setSecondKey(score);

    context.write(combinationKey, score);

  }
}
