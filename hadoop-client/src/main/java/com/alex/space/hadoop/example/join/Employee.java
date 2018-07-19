package com.alex.space.hadoop.example.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

@Data
public class Employee implements Writable {

  private String name;
  private String sex;
  private int age;
  private int depNo;
  private String depName;

  @Override
  public void readFields(DataInput in) throws IOException {
    this.name = Text.readString(in);
    this.sex = Text.readString(in);
    this.age = in.readInt();
    this.depNo = in.readInt();
    this.depName = Text.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.name);
    Text.writeString(out, this.sex);
    out.writeInt(this.age);
    out.writeInt(this.depNo);
    Text.writeString(out, this.depName);
  }

  @Override
  public String toString() {

    return String.format("%s,%s,%s,%s,%s", name, sex, age, depNo, depName);
  }

}
