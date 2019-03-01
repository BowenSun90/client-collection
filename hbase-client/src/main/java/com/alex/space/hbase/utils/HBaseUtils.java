package com.alex.space.hbase.utils;

import com.alex.space.common.utils.StringUtils;
import com.alex.space.hbase.config.HBaseConfig;
import com.alex.space.hbase.factory.HbaseDataAccessException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase API
 *
 * @author Alex Created by Alex on 2018/4/6.
 */
@Slf4j
public class HBaseUtils {

  private static HBaseConfig hBaseConfig = HBaseConfig.getInstance();
  /**
   * HBase conf
   */
  private static Configuration conf = HBaseConfiguration.create();

  /**
   * ScheduledThreadPool
   */
  private static ExecutorService pool = Executors.newScheduledThreadPool(10);

  /**
   * HBase connection
   */
  private static Connection connection = null;

  private static HBaseUtils instance = null;

  /**
   * Init HBase connection
   */
  private HBaseUtils() {
    if (connection == null) {
      try {
        conf.set("hbase.zookeeper.quorum", hBaseConfig.getProperty("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort",
            hBaseConfig.getProperty("hbase.zookeeper.property.clientPort"));

        conf.set("hbase.rootdir", hBaseConfig.getProperty("hbase.rootdir"));

        conf.set("fs.defaultFS", hBaseConfig.getProperty("hbase.fs.defaultFS"));

        connection = ConnectionFactory.createConnection(conf, pool);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static synchronized HBaseUtils getInstance() {
    if (instance == null) {
      instance = new HBaseUtils();
    }
    return instance;
  }

  public Connection getConnection() {
    return connection;
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Create table
   *
   * @param tableName Table name
   * @param columnFamilies Column family name
   */
  public void createTable(String tableName, String[] columnFamilies) throws IOException {
    Admin admin = connection.getAdmin();
    TableName name = TableName.valueOf(tableName);

    if (admin.tableExists(name)) {
      log.warn("Table {} exists.", name);
    } else {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

      for (String columnFamily : columnFamilies) {
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
        desc.addFamily(hColumnDescriptor);
      }
      admin.createTable(desc);
      log.info("Create table {}.", tableName);
    }

    admin.close();
  }

  /**
   * Create table
   *
   * @param tableName Table name
   * @param columnFamilies Column family name
   * @param splitKeys Table split keys
   */
  public void createTable(String tableName, String[] columnFamilies, byte[][] splitKeys)
      throws IOException {
    Admin admin = connection.getAdmin();
    TableName name = TableName.valueOf(tableName);

    if (admin.tableExists(name)) {
      log.warn("Table {} exists.", name);
    } else {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

      for (String columnFamily : columnFamilies) {
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
        desc.addFamily(hColumnDescriptor);
      }
      admin.createTable(desc, splitKeys);
      log.info("Create table {}.", tableName);
    }

    admin.close();
  }

  /**
   * Drop table
   *
   * @param tableName Table name
   */
  public void dropTable(String tableName) throws IOException {
    Admin admin = connection.getAdmin();
    TableName name = TableName.valueOf(tableName);
    if (admin.tableExists(name)) {
      log.warn("Table {} exists, disable and drop table.", name);
      admin.disableTable(name);
      admin.deleteTable(name);
    }

    admin.close();
  }

  /**
   * Put column value
   *
   * @param tableName Table name
   * @param row Row key
   * @param columnFamily Column family name
   * @param column Column name
   * @param value Value
   */
  public void put(String tableName, String row, String columnFamily, String column, String value) {
    try {
      TableName name = TableName.valueOf(tableName);
      Table table = connection.getTable(name);
      Put put = new Put(Bytes.toBytes(row));

      put.addColumn(
          Bytes.toBytes(columnFamily),
          Bytes.toBytes(String.valueOf(column)),
          Bytes.toBytes(value));

      table.put(put);

      table.close();
      log.info("Put table:{}, row:{}, columnFamily:{}, column:{}, value:{}", tableName, row,
          columnFamily, column, value);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Put set of column value
   *
   * @param tableName Table name
   * @param row Row key
   * @param columnFamily Column family name
   * @param columns Array of columns
   * @param values Array of values
   */
  public void put(String tableName, String row, String columnFamily, String[] columns,
      String[] values) {
    try {
      TableName name = TableName.valueOf(tableName);
      Table table = connection.getTable(name);

      List<Put> puts = new ArrayList<Put>(columns.length);

      for (int i = 0; i < columns.length; i++) {

        Put put = new Put(Bytes.toBytes(row));

        put.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]),
            Bytes.toBytes(values[i]));
        puts.add(put);
      }
      table.put(puts);

      table.close();
      log.info("Put table:{}, row:{}, columnFamily:{}, column:{}, value:{}", tableName, row,
          columnFamily, columns, values);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Select by row key
   *
   * @param tableName Table name
   * @param row Row key
   */
  public void selectRow(String tableName, String row) {
    try {
      TableName name = TableName.valueOf(tableName);
      Table table = connection.getTable(name);
      Get get = new Get(row.getBytes());

      Result rs = table.get(get);
      for (Cell cell : rs.rawCells()) {
        printlnCell(cell);
      }

      table.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Scan table records
   *
   * @param tableName Table name
   */
  public void scanAllRecord(String tableName) {
    try {
      TableName name = TableName.valueOf(tableName);

      Table table = connection.getTable(name);
      Scan scan = new Scan();
      ResultScanner rs = table.getScanner(scan);

      for (Result result : rs) {
        for (Cell cell : result.rawCells()) {
          printlnCell(cell);
        }
      }

      table.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Scan table records
   *
   * @param tableName Table name
   * @param prefix rowkey prefix
   */
  public void scanRecord(String tableName, String prefix) {
    StopWatch watch = new StopWatch();
    try {
      watch.start();
      TableName name = TableName.valueOf(tableName);

      Table table = connection.getTable(name);
      Scan scan = new Scan();
      scan.setBatch(1000);
      scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));
      ResultScanner rs = table.getScanner(scan);

      for (Result result : rs) {
        for (Cell cell : result.rawCells()) {
          printlnCell(cell);
        }
      }

      table.close();
      watch.stop();
    } catch (IOException e) {
      e.printStackTrace();
    }

    log.info("Execute time: " + watch.getTime());
  }

  /**
   * Scan table records
   *
   * @param tableName Table name
   * @param column column name prefix
   * @param value column value
   */
  public void scanRecord(String tableName, String column, String value) {
    StopWatch watch = new StopWatch();
    try {
      watch.start();
      TableName name = TableName.valueOf(tableName);

      Table table = connection.getTable(name);
      Scan scan = new Scan();
      scan.setBatch(100);
      scan.setFilter(new ColumnPrefixFilter(Bytes.toBytes(column)));
      if (StringUtils.isEmpty(value)) {
        scan.setFilter(new ValueFilter(CompareOp.EQUAL, new SubstringComparator(value)));
      }
      ResultScanner rs = table.getScanner(scan);

      for (Result result : rs) {
        for (Cell cell : result.rawCells()) {
          printlnCell(cell);
        }
      }

      table.close();
      watch.stop();
    } catch (IOException e) {
      e.printStackTrace();
    }

    log.info("Execute time: " + watch.getTime());
  }

  /**
   * Delete by row key
   *
   * @param tableName Table name
   * @param row Row key
   */
  public void delete(String tableName, String row) {

    try {
      TableName name = TableName.valueOf(tableName);
      Table table = connection.getTable(name);

      Delete delete = new Delete(Bytes.toBytes(row));
      table.delete(delete);

      table.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Delete by row key
   *
   * @param tableName Table name
   * @param rows Row key list
   */
  public void delete(String tableName, List<String> rows) {

    try {
      TableName name = TableName.valueOf(tableName);
      Table table = connection.getTable(name);

      List<Delete> deletes = new ArrayList<>();

      for (String row : rows) {
        Delete delete = new Delete(Bytes.toBytes(row));
        deletes.add(delete);
      }
      table.delete(deletes);

      table.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void printlnCell(Cell cell) {
    System.out.print(Bytes.toString(CellUtil.cloneRow(cell)) + " ");
    System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":");
    System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + " ");
    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
    System.out.print(cell.getTimestamp() + " ");
  }

  /**
   * Put in batch
   */
  public void putAll() throws IOException {

    HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("people"));
    HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");
    columnDescriptor.setMaxVersions(3);

    htd.addFamily(columnDescriptor);
    htd.addFamily(new HColumnDescriptor("data"));

    admin.createTable(htd);
    admin.close();

    HTable table = (HTable) connection.getTable(TableName.valueOf("people"));

    List<Put> puts = new ArrayList<Put>(10000);
    for (int i = 1; i <= 100001; i++) {
      Put put = new Put(Bytes.toBytes("rk" + i));
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes("" + i));
      puts.add(put);
      if (i % 10000 == 0) {
        table.put(puts);
        puts = new ArrayList<Put>(10000);
      }
    }
  }

  /**
   * Test API
   */
  public static void main(String[] args) {

    try {
      HBaseUtils hBaseUtils = HBaseUtils.getInstance();

      String table = "tb_user";
      String[] columnFamilies = {"cf_info", "cf_profile"};

      hBaseUtils.dropTable(table);

      hBaseUtils.createTable(table, columnFamilies);

      String[] columns = {"name", "age"};
      String[] values = {"tom", "20"};
      String row1 = "row1";
      hBaseUtils.put(table, row1, columnFamilies[0], columns, values);
      hBaseUtils.put(table, row1, columnFamilies[1], "tel", "123456");

      hBaseUtils.selectRow(table, row1);

      hBaseUtils.scanAllRecord(table);

      log.info("Process finish.");

      System.exit(0);
    } catch (Exception e) {
      log.error("Process finish with exception", e);
      System.exit(1);
    }
  }

  public static HTableInterface getHTable(String tableName, Configuration configuration,
      Charset charset, HTableInterfaceFactory tableFactory) {

    HTableInterface t;
    try {
      if (tableFactory != null) {
        t = tableFactory.createHTableInterface(configuration, tableName.getBytes(charset));
      } else {
        t = new HTable(configuration, tableName.getBytes(charset));
      }

      return t;
    } catch (Exception ex) {
      throw new HbaseDataAccessException(ex);
    }
  }


  public static void releaseTable(String tableName, HTableInterface table,
      HTableInterfaceFactory tableFactory) {
    try {
      if (table == null) {
        return;
      }

      if (tableFactory != null) {
        tableFactory.releaseHTableInterface(table);
      } else {
        table.close();
      }

    } catch (IOException ex) {
      throw new HbaseDataAccessException(ex);
    }
  }
}
