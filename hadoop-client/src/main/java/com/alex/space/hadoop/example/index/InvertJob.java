package com.alex.space.hadoop.example.index;

import com.alex.space.hadoop.utils.HdfsUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 有一批电话清单，记录了用户A拨打给用户B的记录
 *
 * 做一个倒排索引，记录拨打给用户B所有的用户A
 *
 * @author Alex
 */
public class InvertJob extends Configured implements Tool {

  public static final String INPUT_PATH = "hdfs://localhost:9000/invert/input/info.txt";
  public static final String OUTPUT_PATH = "hdfs://localhost:9000/invert/output";

  public InvertJob() throws IOException, URISyntaxException {
    init();
  }

  public void init() throws IOException, URISyntaxException {
    HdfsUtils.init("data/invert/invert.txt", INPUT_PATH, OUTPUT_PATH);
  }

  @Override
  public int run(String[] arg0) throws Exception {
    Job job = new Job(getConf(), "倒排索引");
    job.setJarByClass(InvertApp.class);

    FileInputFormat.addInputPath(job, new Path(INPUT_PATH));

    job.setMapperClass(InvertMap.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    job.setReducerClass(InvertReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

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
