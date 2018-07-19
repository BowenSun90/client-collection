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
public class User implements Writable, DBWritable {

  private int id;
  private String name;

  @Override
  public void readFields(ResultSet resultSet) throws SQLException {
    this.id = resultSet.getInt(1);
    this.name = resultSet.getString(2);
  }

  @Override
  public void write(PreparedStatement preparedstatement) throws SQLException {
    preparedstatement.setInt(1, this.id);
    preparedstatement.setString(2, this.name);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.id = dataInput.readInt();
    this.name = Text.readString(dataInput);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(this.id);
    Text.writeString(dataOutput, this.name);
  }

  @Override
  public String toString() {
    return "User [id=" + id + ", name=" + name + "]";
  }
}
