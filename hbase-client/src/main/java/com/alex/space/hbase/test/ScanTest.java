package com.alex.space.hbase.test;

import com.alex.space.hbase.utils.HBaseUtils;
import java.io.IOException;

/**
 * @author Alex Created by Alex on 2018/8/25.
 */
public class ScanTest {

  public static void main(String[] args) throws IOException {
    HBaseUtils hBaseUtils = HBaseUtils.getInstance();

    String tableName = "trait_info";

    hBaseUtils.scanRecord(tableName, "user_1");

    hBaseUtils.scanRecord(tableName, "abc", "abc");

    hBaseUtils.scanRecord(tableName, "abc", "");
  }

}
