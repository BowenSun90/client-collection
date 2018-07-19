package com.alex.space.hadoop.example.join;

import com.alex.space.hadoop.utils.HdfsUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Reduce-side join
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class ReduceJoinApp {

  // 定义输入路径
  private static final String INPUT_PATH = "hdfs://localhost:9000/join/tb_*";
  private static final String INPUT_PATH1 = "hdfs://localhost:9000/join/tb_a1.txt";
  private static final String INPUT_PATH2 = "hdfs://localhost:9000/join/tb_b1.txt";
  // 定义输出路径
  private static final String OUT_PATH = "hdfs://localhost:9000/join/reduce";

  @SuppressWarnings("deprecation")
  public static void main(String[] args)
      throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

    HdfsUtils.init("data/join/tb_a1.txt", INPUT_PATH1, OUT_PATH);
    HdfsUtils.init("data/join/tb_b1.txt", INPUT_PATH2, OUT_PATH);

    // 创建配置信息
    Configuration conf = new Configuration();

    // 创建任务
    Job job = new Job(conf, ReduceJoinApp.class.getName());

    // 1.1 设置输入目录和设置输入数据格式化的类
    FileInputFormat.setInputPaths(job, INPUT_PATH);
    job.setInputFormatClass(TextInputFormat.class);

    // 1.2 设置自定义Mapper类和设置map函数输出数据的key和value的类型
    job.setMapperClass(ReduceJoinMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // 1.3 设置分区和reduce数量(reduce的数量，和分区的数量对应，因为分区为一个，所以reduce的数量也是一个)
    job.setPartitionerClass(HashPartitioner.class);
    job.setNumReduceTasks(1);

    // 1.4 排序
    // 1.5 归约
    // 2.1 Shuffle把数据从Map端拷贝到Reduce端。
    // 2.2 指定Reducer类和输出key和value的类型
    job.setReducerClass(ReduceJoinReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // 2.3 指定输出的路径和设置输出的格式化类
    FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
    job.setOutputFormatClass(TextOutputFormat.class);

    // 提交作业 退出
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
