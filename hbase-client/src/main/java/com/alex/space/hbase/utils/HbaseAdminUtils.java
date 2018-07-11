package com.alex.space.hbase.utils;

import com.alex.space.hbase.config.HBaseConstants;
import com.alex.space.hbase.factory.HbaseHelper;
import com.alex.space.hbase.factory.PooledHTableInterfaceFactory;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBaseAdmin Utils
 *
 * @author alex Created by Alex on 2018/6/3.
 */
@Slf4j
public class HbaseAdminUtils {

  private static HbaseAdminUtils instance = null;

  public static synchronized HbaseAdminUtils getInstance() {
    if (instance == null) {
      instance = new HbaseAdminUtils();
    }
    return instance;
  }

  /**
   * HBaseAdmin, hbase client api
   */
  private HBaseAdmin hBaseAdmin = null;

  public void init() throws MasterNotRunningException, ZooKeeperConnectionException {
    this.initHbaseAdmin();
  }

  public HBaseAdmin gethBaseAdmin() {
    return hBaseAdmin;
  }

  private synchronized void initHbaseAdmin()
      throws MasterNotRunningException, ZooKeeperConnectionException {
    PooledHTableInterfaceFactory factory = HbaseHelper.getInstance().getTableFactory();

    if (hBaseAdmin == null) {
      hBaseAdmin = new HBaseAdmin(factory.getHConnection());
    }
  }

  /**
   * Check table exist
   *
   * @param tableName Table name
   */
  public boolean tableExists(String tableName) throws IOException {
    if (hBaseAdmin == null) {
      initHbaseAdmin();
    }

    return hBaseAdmin.tableExists(tableName);
  }

  /**
   * Create table
   *
   * @param tableName Table name
   */
  public boolean createTable(String tableName) throws IOException {
    if (tableExists(tableName)) {
      return true;
    }

    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
    descriptor.addFamily(new HColumnDescriptor(HBaseConstants.HBASE_DEFAULT_FAMILY));

    hBaseAdmin.createTable(descriptor);

    return tableExists(tableName);
  }

  /**
   * Create table with pre-splitting keys
   *
   * @param tableName Table name
   * @param splitKeys Split keys
   */
  public boolean createTable(String tableName, byte[][] splitKeys) throws IOException {
    if (tableExists(tableName)) {
      return true;
    }

    HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
    hTableDescriptor.setMemStoreFlushSize(HBaseConstants.MEMSTORE_FLUSH_SIZE);
    hTableDescriptor.setCompactionEnabled(HBaseConstants.COMPACTION_ENABLED);
    hTableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.HBASE_DEFAULT_FAMILY));

    hBaseAdmin.createTable(hTableDescriptor, splitKeys);

    return tableExists(tableName);
  }

  /**
   * Drop table
   *
   * @param tableName Table name
   */
  public void dropTable(String tableName) throws IOException {
    if (!tableExists(tableName)) {
      return;
    }

    hBaseAdmin.disableTable(TableName.valueOf(tableName));
    hBaseAdmin.deleteTable(TableName.valueOf(tableName));
  }

  /**
   * Test API
   */
  public static void main(String args[]) throws IOException {

    try {
      byte[][] regions = new byte[][]{
          Bytes.toBytes("1"),
          Bytes.toBytes("2"),
          Bytes.toBytes("3"),
          Bytes.toBytes("4"),
          Bytes.toBytes("5")};

      HbaseAdminUtils.getInstance().createTable("tb_demo", regions);
      log.info("Process finish.");

      System.exit(0);
    } catch (Exception e) {
      log.error("Process finish with exception", e);
      System.exit(1);
    }
  }

}

