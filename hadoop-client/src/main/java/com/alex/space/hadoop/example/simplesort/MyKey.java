package com.alex.space.hadoop.example.simplesort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义Key，自定义排序方法
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class MyKey implements WritableComparable<MyKey> {

  public long myk2;
  public long myv2;

  MyKey(long myk2, long myv2) {
    this.myk2 = myk2;
    this.myv2 = myv2;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.myk2 = in.readLong();
    this.myv2 = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(myk2);
    out.writeLong(myv2);
  }

  @Override
  public int compareTo(MyKey myk2) {
    // myk2之差>0 返回-1 <0 返回1 代表 myk2列降序
    // myk2之差<0 返回-1 >0 返回1 代表 myk2列升序
    long temp = this.myk2 - myk2.myk2;
    if (temp > 0) {
      return -1;
    } else if (temp < 0) {
      return 1;
    }
    // 控制myv2升序
    return (int) (this.myv2 - myk2.myv2);
  }
}
