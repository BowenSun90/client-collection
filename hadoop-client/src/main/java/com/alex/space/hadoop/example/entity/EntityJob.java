package com.alex.space.hadoop.example.entity;

import com.alex.space.hadoop.utils.HdfsUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * entity job
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class EntityJob extends Configured implements Tool {

  public static final String INPUT_PATH = "hdfs://localhost:9000/entity/input/info.txt";
  public static final String OUTPUT_PATH = "hdfs://localhost:9000/entity/output";

  public EntityJob() throws IOException, URISyntaxException {
    init();
  }

  public void init() throws IOException, URISyntaxException {
    HdfsUtils.init("data/entity/entity.txt", INPUT_PATH, OUTPUT_PATH);
  }

  @Override
  public int run(String[] arg0) throws Exception {
    Job job = new Job(getConf(), "自定义数据类型");
    job.setJarByClass(EntityApp.class);

    FileInputFormat.addInputPath(job, new Path(INPUT_PATH));

    job.setMapperClass(EntityMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MyEntity.class);

    job.setNumReduceTasks(1);

    job.setReducerClass(EntityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MyEntity.class);

    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    return 0;
  }

}
