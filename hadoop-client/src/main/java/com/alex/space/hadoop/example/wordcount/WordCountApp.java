package com.alex.space.hadoop.example.wordcount;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 单词计数
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class WordCountApp extends Configured implements Tool {

  public static final String INPUT_PATH = "hdfs://localhost:9000/wordcount/input/words.txt";

  public static final String OUTPUT_PATH = "hdfs://localhost:9000/wordcount/output";

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    try {
      int res = ToolRunner.run(conf, new WordCountApp(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    // 首先删除输出路径的已有生成文件
    FileSystem fs = FileSystem.get(new URI(INPUT_PATH), getConf());
    Path outPath = new Path(OUTPUT_PATH);
    if (fs.exists(outPath)) {
      fs.delete(outPath, true);
    }

    Job job = new Job(getConf(), "WordCount");
    // 设置输入目录
    FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
    // 设置自定义Mapper
    job.setMapperClass(WordMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    // Map端Reduce
    job.setCombinerClass(WordReducer.class);
    // 设置自定义Reducer
    job.setReducerClass(WordReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setPartitionerClass(WordPartitioner.class);
    job.setNumReduceTasks(2);

    // 设置输出目录
    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    return 0;
  }
}
