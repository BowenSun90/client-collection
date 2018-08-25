package com.alex.space.hbase.test;

import com.alex.space.common.utils.StringUtils;
import com.alex.space.hbase.utils.HBaseUtils;
import java.io.IOException;
import java.time.LocalTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 测试，多线程插入Hbase
 *
 * @author Alex Created by Alex on 2018/8/24.
 */
@Slf4j
public class StressTest {

  private char[] chars = "abcde".toCharArray();

  public static void main(String[] args) throws IOException {

    String tableName = "trait_info";
    String[] columnFamilies = {"d"};

    HBaseUtils hBaseUtils = HBaseUtils.getInstance();
    log.info("Create table");
    byte[][] regions = new byte[10][1];
    for (int i = 0; i < 10; i++) {
      regions[i] = Bytes.toBytes("user_" + i);
    }

    hBaseUtils.createTable(tableName, columnFamilies, regions);

    log.info("Insert data");
    ExecutorService pool = Executors.newFixedThreadPool(15);
    for (int i = 0; i < 30; i++) {
      pool.submit(new StressTest().new WriteThread(tableName, columnFamilies, 5000));
    }

    pool.shutdown();

    log.info("Process finish.");

  }

  class WriteThread implements Runnable {

    int count;
    String tableName;
    String[] columnFamilies;

    WriteThread(String tableName, String[] columnFamilies, int count) {
      this.tableName = tableName;
      this.columnFamilies = columnFamilies;
      this.count = count;
    }

    @Override
    public void run() {
      HBaseUtils hBaseUtils = HBaseUtils.getInstance();

      for (int i = 0; i < count; i++) {
        int columnNum = ThreadLocalRandom.current().nextInt(10, 20);
        String[] columns = new String[columnNum];
        String[] values = new String[columnNum];
        for (int j = 0; j < columnNum - 1; j++) {
          columns[j] = StringUtils
              .generateStringMessage(ThreadLocalRandom.current().nextInt(1, 5), chars)
              .toLowerCase();

          values[j] = StringUtils
              .generateStringMessage(ThreadLocalRandom.current().nextInt(5, 15), chars)
              .toLowerCase();
        }
        columns[columnNum - 1] = "lastUpdateTime";
        values[columnNum - 1] = LocalTime.now().toString();

        int userId = ThreadLocalRandom.current().nextInt(100000);
        String rowKey =
            "user" + (columnNum % 15 != 0 ? "_" : "") + StringUtils.leftPad(4, userId);

        hBaseUtils.put(tableName, rowKey, columnFamilies[0], columns, values);
        log.info("Put: " + i + "[" + userId + "]");
      }

    }
  }

}
