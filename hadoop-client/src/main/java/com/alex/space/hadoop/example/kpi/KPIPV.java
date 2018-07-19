package com.alex.space.hadoop.example.kpi;

import com.alex.space.hadoop.utils.HdfsUtils;
import java.io.IOException;
import java.net.URISyntaxException;
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
 * Kpi Analysis:pv
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class KPIPV {

  public static class KPIPVMapper extends MapReduceBase implements Mapper {

    private IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Object value, OutputCollector output, Reporter arg3)
        throws IOException {
      KPI kpi = KPI.filterPVs(value.toString());
      if (kpi.isValid()) {
        word.set(kpi.getRequest());
        output.collect(word, one);
      }
    }
  }


  public static class KPIPVReducer extends MapReduceBase implements Reducer {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Object key, Iterator values, OutputCollector output, Reporter arg3)
        throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += ((IntWritable) values.next()).get();
      }
      result.set(sum);
      output.collect(key, result);
    }

  }

  public static void main(String[] args) throws IOException, URISyntaxException {
    String input = "hdfs://localhost:9000/log_kpi/log/access.log";
    String output = "hdfs://localhost:9000/log_kpi/pv";

    HdfsUtils.init("data/kpi/access.log", input, output);

    JobConf conf = new JobConf(KPIPV.class);
    conf.setJobName("KPIPV");
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(KPIPVMapper.class);
    conf.setCombinerClass(KPIPVReducer.class);
    conf.setReducerClass(KPIPVReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(input));
    FileOutputFormat.setOutputPath(conf, new Path(output));

    JobClient.runJob(conf);
    System.exit(0);
  }

}
