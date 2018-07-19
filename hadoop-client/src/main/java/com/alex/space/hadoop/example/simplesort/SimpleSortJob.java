package com.alex.space.hadoop.example.simplesort;

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
 * Simple sort job
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class SimpleSortJob extends Configured implements Tool {

  public static final String INPUT_PATH = "hdfs://localhost:9000/sort/input/info.txt";
  public static final String OUTPUT_PATH = "hdfs://localhost:9000/sort/output";

  public SimpleSortJob() throws IOException, URISyntaxException {
    init();
  }

  public void init() throws IOException, URISyntaxException {
    HdfsUtils.init("data/sort/partition.txt", INPUT_PATH, OUTPUT_PATH);
  }

  @Override
  public int run(String[] arg0) throws Exception {
    Job job = new Job(getConf(), "SimpleSort");
    job.setJarByClass(SimpleSortApp.class);

    FileInputFormat.addInputPath(job, new Path(INPUT_PATH));

    job.setMapperClass(SimpleSortMap.class);
    job.setMapOutputKeyClass(MyKey.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setNumReduceTasks(1);

    job.setReducerClass(SimpleSortReduce.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);

    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    return 0;
  }

}
