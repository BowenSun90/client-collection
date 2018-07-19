package com.alex.space.hadoop.example.kpi;

import com.alex.space.hadoop.utils.HdfsUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Kpi Analysis:time
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class KPITime {

  public static class KPITimeMapper extends MapReduceBase implements
      Mapper<Object, Text, Text, IntWritable> {

    private IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, OutputCollector<Text, IntWritable> output,
        Reporter reporter)
        throws IOException {
      KPI kpi = KPI.filterTime(value.toString());
      if (kpi.isValid()) {
        try {
          word.set(kpi.getTime_local_Date_hour());
          output.collect(word, one);
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static class KPITimeReducer extends MapReduceBase implements
      Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      result.set(sum);
      output.collect(key, result);
    }
  }

  public static void main(String[] args) throws IOException, URISyntaxException {
    String input = "hdfs://localhost:9000/log_kpi/log/access.log";
    String output = "hdfs://localhost:9000/log_kpi/time";

    HdfsUtils.init("data/kpi/access.log", input, output);

    JobConf conf = new JobConf(KPIPV.class);
    conf.setJobName("KPITime");
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(KPITimeMapper.class);
    conf.setCombinerClass(KPITimeReducer.class);
    conf.setReducerClass(KPITimeReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(input));
    FileOutputFormat.setOutputPath(conf, new Path(output));

    JobClient.runJob(conf);
    System.exit(0);
  }

}
