package com.alex.space.hbase.utils;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase create table Utils
 *
 * @author alex Created by Alex on 2018/4/6.
 */
@Slf4j
public class HBaseTableUtils {

  /**
   * Init table
   *
   * @param tableName Table name
   * @param regionNum Region number
   */
  public static void initTable(String tableName, int regionNum) throws IOException {
    initTable(tableName, regionNum, false);
  }

  /**
   * Init table
   *
   * @param tableName Table name
   * @param regionNum Region number
   * @param isDelTable Delete table or not
   */
  public static void initTable(String tableName, int regionNum, boolean isDelTable)
      throws IOException {
    byte[][] regions = new byte[regionNum][1];
    for (int i = 0; i < regionNum; i++) {
      regions[i] = Bytes.toBytes("" + buildHashCodeByIndex(i));
    }

    boolean existTable = HbaseAdminUtils.getInstance().tableExists(tableName);
    if (existTable) {
      if (isDelTable) {
        HbaseAdminUtils.getInstance().dropTable(tableName);
        HbaseAdminUtils.getInstance().createTable(tableName, regions);
      }

      log.warn("Table {} exists.", tableName);
    } else {
      HbaseAdminUtils.getInstance().createTable(tableName, regions);
    }
  }

  /**
   * Pre-splitting keys use index of region to split key
   */
  private static char buildHashCodeByIndex(int index) {
    int b;
    if (index < 10) {
      b = index + '0';
    } else if (index < 36) {
      b = index - 10 + 'A';
    } else if (index < 62) {
      b = index - 36 + 'a';
    } else {
      b = '0';
    }
    return (char) b;
  }

  /**
   * Test API
   */
  public static void main(String args[]) throws Exception {
    System.out.println("Start create table: tableName - " + args[0] + ", regionNum - " + args[1]);
    String tableName = args[0];
    String regionNumStr = args[1];
    int regionNum = Integer.valueOf(regionNumStr);

    initTable(tableName, regionNum, true);
    System.out.println("OK");
  }

}
