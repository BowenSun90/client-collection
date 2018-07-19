package com.alex.space.hadoop.example.secondsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 一定要实现WritableComparable接口，并且实现compareTo()方法的比较策略。这个用于MapReduce的第一次默认排序
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class CombinationKey implements WritableComparable<CombinationKey> {

  private Text firstKey;
  private IntWritable secondKey;

  /**
   * 无参构造函数
   */
  public CombinationKey() {
    this.firstKey = new Text();
    this.secondKey = new IntWritable();
  }

  /**
   * 有参构造函数
   */
  public CombinationKey(Text firstKey, IntWritable secondKey) {
    this.firstKey = firstKey;
    this.secondKey = secondKey;
  }

  public Text getFirstKey() {
    return firstKey;
  }

  public void setFirstKey(Text firstKey) {
    this.firstKey = firstKey;
  }

  public IntWritable getSecondKey() {
    return secondKey;
  }

  public void setSecondKey(IntWritable secondKey) {
    this.secondKey = secondKey;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.firstKey.write(out);
    this.secondKey.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.firstKey.readFields(in);
    this.secondKey.readFields(in);
  }

  /**
   * 自定义比较策略 注意：该比较策略用于MapReduce的第一次默认排序 也就是发生在Map端的sort阶段 发生地点为环形缓冲区(可以通过io.sort.mb进行大小调整)
   */
  @Override
  public int compareTo(CombinationKey combinationKey) {
    System.out.println("------------------------CombineKey flag-------------------");
    return this.firstKey.compareTo(combinationKey.getFirstKey());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((firstKey == null) ? 0 : firstKey.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CombinationKey other = (CombinationKey) obj;
    if (firstKey == null) {
      if (other.firstKey != null) {
        return false;
      }
    } else if (!firstKey.equals(other.firstKey)) {
      return false;
    }
    return true;
  }


}

