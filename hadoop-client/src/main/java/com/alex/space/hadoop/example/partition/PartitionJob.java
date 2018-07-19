package com.alex.space.hadoop.example.partition;

import com.alex.space.hadoop.utils.HdfsUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * PartitionJob
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class PartitionJob extends Configured implements Tool {

  public static final String INPUT_PATH = "hdfs://localhost:9000/partition/input/info.txt";
  public static final String OUTPUT_PATH = "hdfs://localhost:9000/partition/output";

  public PartitionJob() throws IOException, URISyntaxException {
    init();
  }

  public void init() throws IOException, URISyntaxException {
    HdfsUtils.init("data/partition/partition.txt", INPUT_PATH, OUTPUT_PATH);
  }

  @Override
  public int run(String[] arg0) throws Exception {
    Job job = new Job(getConf(), "自定义分区");
    job.setJarByClass(PartitionApp.class);

    FileInputFormat.addInputPath(job, new Path(INPUT_PATH));

    job.setMapperClass(PartitionMap.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setPartitionerClass(MyPartition.class);
    job.setNumReduceTasks(3);

    job.setReducerClass(PartitionReduce.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);

    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    return 0;
  }

}
