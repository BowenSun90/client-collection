package com.alex.space.hadoop.example.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义Partition
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DefinedPartition extends Partitioner<CombinationKey, IntWritable> {

  /**
   * 数据输入来源：map输出
   *
   * 这里根据组合键的第一个值作为分区
   *
   * 如果不自定义分区的话，MapReduce会根据默认的Hash分区方法将整个组合键相等的分到一个分区中
   *
   * @param key map输出键值
   * @param value map输出value值
   * @param numPartitions 分区总数，即reduce task个数
   */
  @Override
  public int getPartition(CombinationKey key, IntWritable value, int numPartitions) {
    System.out.println("---------------------进入自定义分区---------------------");
    return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }

}
