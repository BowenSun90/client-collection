package com.alex.space.hbase;

import com.alex.space.hbase.utils.HBaseUtils;
import java.time.LocalTime;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Alex Created by Alex on 2018/6/22.
 */
@Slf4j
public class Test {

  public static void main(String[] args) {
    Random r = new Random();

    HBaseUtils hBaseUtils = HBaseUtils.getInstance();

    String table = "crowd_bitmap";
    String[] columnFamilies = {"d"};
    String[] columns = {"tenant_id", "object_id", "object_type", "rowkey", "lastUpdateTime",
        "bitmap"};
    for (int i = 10001; i < 20000; i++) {
      String rowKey = "000000" + i;
      String[] values = {
          "talkingdata0000",
          "100000000" + i,
          "biz_tag000000",
          rowKey,
          LocalTime.now().toString(),
          "10000000000000000000000000000000000000000000000000000000000000000000000000000000000"
              + i};
      hBaseUtils.put(table, rowKey, columnFamilies[0], columns, values);
    }

    log.info("Process finish.");
  }

}
