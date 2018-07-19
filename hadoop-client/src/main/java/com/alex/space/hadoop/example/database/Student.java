package com.alex.space.hadoop.example.database;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.Data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

@Data
public class Student implements Writable, DBWritable {

  private int id;
  private String name;

  @Override
  public void readFields(ResultSet result) throws SQLException {
    this.id = result.getInt(1);
    this.name = result.getString(2);
  }

  @Override
  public void write(PreparedStatement state) throws SQLException {
    state.setInt(1, this.id);
    state.setString(2, this.name);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
    this.name = Text.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.id);
    Text.writeString(out, this.name);
  }

  @Override
  public String toString() {
    return new String(this.id + " " + this.name);
  }

}
