package com.alex.space.hadoop.example.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map-side join
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, NullWritable, Employee> {

  private Map<Integer, String> joinData = new HashMap<Integer, String>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {

    // 预处理把要关联的文件加载到缓存中
    URI[] paths = DistributedCache.getCacheFiles(context.getConfiguration());
    // 我们这里只缓存了一个文件，所以取第一个即可，创建BufferReader去读取
    BufferedReader reader = new BufferedReader(new FileReader(paths[0].toString()));

    String str = null;
    try {
      // 一行一行读取
      while ((str = reader.readLine()) != null) {
        // 对缓存中的表进行分割
        String[] splits = str.split("\t");
        // 把字符数组中有用的数据存在一个Map中
        joinData.put(Integer.parseInt(splits[0]), splits[1]);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      reader.close();
    }

  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 获取从HDFS中加载的表
    String[] values = value.toString().split("\t");
    // 创建Employee对象
    Employee emplyee = new Employee();
    // 设置属性
    emplyee.setName(values[0]);
    emplyee.setSex(values[1]);
    emplyee.setAge(Integer.parseInt(values[2]));
    // 获取关联字段depNo，这个字段是关键
    int depNo = Integer.parseInt(values[3]);
    // 根据depNo从内存中的关联表中获取要关联的属性depName
    String depName = joinData.get(depNo);
    // 设置depNo
    emplyee.setDepNo(depNo);
    // 设置depName
    emplyee.setDepName(depName);

    // 写出去
    context.write(NullWritable.get(), emplyee);
  }
}
