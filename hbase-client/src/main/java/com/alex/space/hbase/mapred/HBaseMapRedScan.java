package com.alex.space.hbase.mapred;

import com.alex.space.hbase.config.HBaseConstants.MapRedConstants;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 * Hbase MapReduce Scan
 *
 * @author Alex Created by Alex on 2018/7/26.
 */
@Slf4j
public class HBaseMapRedScan {

  static class ScanMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

    private int numRecords = 0;
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
      // extract userKey from the compositeKey (userId + counter)
      ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);
      try {
        context.write(userKey, ONE);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      numRecords++;
      if ((numRecords % 10000) == 0) {
        context.setStatus("mapper processed " + numRecords + " records so far");
      }
    }
  }

  public static class ScanReducer extends
      TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      Put put = new Put(key.get());
      put.addColumn(MapRedConstants.FAMILY_NAME, MapRedConstants.QUALIFIER_NAME,
          Bytes.toBytes(sum));

      log.info(String.format("stats: key: %d, count: %d", Bytes.toInt(key.get()), sum));
      context.write(key, put);
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Job job = new Job(conf, "HbaseMRScan");
    job.setJarByClass(HBaseMapRedScan.class);

    Scan scan = new Scan();
    scan.addFamily(MapRedConstants.FAMILY_NAME);
    scan.setFilter(new FirstKeyOnlyFilter());

    TableMapReduceUtil.initTableMapperJob(MapRedConstants.INPUT_TABLE_NAME,
        scan,
        ScanMapper.class,
        ImmutableBytesWritable.class,
        IntWritable.class,
        job);

    TableMapReduceUtil
        .initTableReducerJob(MapRedConstants.OUTPUT_TABLE_NAME,
            ScanReducer.class,
            job);

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}