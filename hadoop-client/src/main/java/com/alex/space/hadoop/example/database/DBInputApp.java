package com.alex.space.hadoop.example.database;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Database input application
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DBInputApp {

  public static final String OUTPUT_PATH = "hdfs://localhost:9000/db";

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 创建配置信息
    Configuration conf = new Configuration();

    // 通过conf创建数据库配置信息
    DBConfiguration.configureDB(conf,
        "org.postgresql.Driver",
        "jdbc:postgresql://127.0.0.1:5432/db",
        "postgres",
        "123456");

    // 创建任务
    Job job = new Job(conf, DBInputApp.class.getName());

    // 1.1 设置输入数据格式化的类和设置数据来源
    job.setInputFormatClass(DBInputFormat.class);
    DBInputFormat
        .setInput(job, Student.class, "student_info", null, null, new String[]{"id", "name"});

    // 1.2 设置自定义的Mapper类和Mapper输出的key和value的类型
    job.setMapperClass(DBInputMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    // 1.3 设置分区和reduce数量(reduce的数量和分区的数量对应，因为分区只有一个，所以reduce的个数也设置为一个)
    job.setPartitionerClass(HashPartitioner.class);
    job.setNumReduceTasks(1);

    // 1.4 排序、分组
    // 1.5 归约
    // 2.1 Shuffle把数据从Map端拷贝到Reduce端

    // 2.2 指定Reducer类和输出key和value的类型
    job.setReducerClass(DBInputReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    // 2.3 指定输出的路径和设置输出的格式化类
    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
    job.setOutputFormatClass(TextOutputFormat.class);

    // 提交作业 然后关闭虚拟机正常退出
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
