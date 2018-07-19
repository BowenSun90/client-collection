package com.alex.space.hadoop.example.join;

import com.alex.space.hadoop.utils.HdfsUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Map-side join
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class MapJoinApp {

  /**
   * 定义输入路径
   */
  private static String INPUT_PATH1 = "hdfs://localhost:9000/join/input/tb_a.txt";
  /**
   * 加载到内存的表的路径
   */
  private static String INPUT_PATH2 = "files/tb_b.txt";
  /**
   * 定义输出路径
   */
  private static String OUT_PATH = "hdfs://localhost:9000/join/out";

  public static void main(String[] args)
      throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
    HdfsUtils.init("data/join/tb_a.txt", INPUT_PATH1, OUT_PATH);

    // 创建配置信息
    Configuration conf = new Configuration();

    // 添加到内存中的文件(随便添加多少个文件)
    DistributedCache.addCacheFile(new Path(INPUT_PATH2).toUri(), conf);

    // 创建任务
    Job job = new Job(conf, MapJoinApp.class.getName());
    job.setJarByClass(MapJoinApp.class);
    // 1.1 设置输入目录和设置输入数据格式化的类
    FileInputFormat.setInputPaths(job, INPUT_PATH1);
    // job.setInputFormatClass(TextInputFormat.class);

    // 1.2 设置自定义Mapper类和设置map函数输出数据的key和value的类型
    job.setMapperClass(MapJoinMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Employee.class);

    // 1.3 设置分区和reduce数量
    // job.setPartitionerClass(HashPartitioner.class);
    job.setNumReduceTasks(1);

    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);

    FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));

    // 提交作业 退出
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
