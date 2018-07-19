package com.alex.space.hadoop.example.comparator;

import com.alex.space.hadoop.utils.HdfsUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Comparator job
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class ComparatorJob extends Configured implements Tool {

  public static final String INPUT_PATH = "hdfs://localhost:9000/comparator/input/info.txt";
  public static final String OUTPUT_PATH = "hdfs://localhost:9000/comparator/output";

  public ComparatorJob() throws IOException, URISyntaxException {
    init();
  }

  public void init() throws IOException, URISyntaxException {
    HdfsUtils.init("data/compare/comparator.txt", INPUT_PATH, OUTPUT_PATH);
  }

  @Override
  public int run(String[] arg0) throws Exception {
    Job job = new Job(getConf(), "根据Value排序");
    job.setJarByClass(ComparatorApp.class);

    FileInputFormat.addInputPath(job, new Path(INPUT_PATH));

    job.setMapperClass(ComparatorMap.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    job.setReducerClass(ComparatorReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setSortComparatorClass(MyComparator.class);

    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

    Date start = new Date();
    job.waitForCompletion(true);
    Date end = new Date();

    float time = (float) ((end.getTime() - start.getTime()) / 1000.0);
    System.out.println("Job 开始的时间为：" + start);
    System.out.println("Job 结束的时间为：" + end);
    System.out.println("Job 经历的时间为：" + time + "s");

    System.out.println("Job 是否成功：" + job.isSuccessful());
    System.out.println("Job MAP输入的行数：" + job.getCounters()
        .findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue());
    System.out.println("Job MAP输出的行数：" + job.getCounters()
        .findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue());
    System.out.println("Job REDUCE输入的行数：" + job.getCounters()
        .findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_INPUT_RECORDS").getValue());
    System.out.println("Job REDUCE输出的行数：" + job.getCounters()
        .findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue());

    return 0;
  }

}
