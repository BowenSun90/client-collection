package com.alex.space.hadoop.example.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Data;
import org.apache.hadoop.io.Writable;

/**
 * 自定义数据类型
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
@Data
public class MyEntity implements Writable {

  long upData;
  long downData;
  long upFlow;
  long downFlow;

  public MyEntity(String upData1, String downData1, String upFlow1, String downFlow1) {
    this.upData = Long.parseLong(upData1);
    this.upData = Long.parseLong(downData1);
    this.upFlow = Long.parseLong(upFlow1);
    this.downFlow = Long.parseLong(downFlow1);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.upData = in.readLong();
    this.downData = in.readLong();
    this.upFlow = in.readLong();
    this.downFlow = in.readLong();
    System.out.println(
        "upData:" + upData + "downData:" + downData + "upFlow:" + upFlow + "downFlow:" + downFlow);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(upData);
    out.writeLong(downData);
    out.writeLong(upFlow);
    out.writeLong(upFlow);
    System.out.println(
        "upData:" + upData + "downData:" + downData + "upFlow:" + upFlow + "downFlow:" + downFlow);
  }

  @Override
  public String toString() {
    return upData + "\t" + downData + "\t" + upFlow + "\t" + downFlow;
  }
}
