package com.alex.space.hbase.hfile;

import com.alex.space.hbase.utils.HBaseUtils;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * @author Alex Created by Alex on 2018/9/19.
 */
@Slf4j
public class ClientSideScanner {

  private Configuration conf;

  private Connection connection;

  private ExecutorService pool = Executors.newScheduledThreadPool(10);

  private Scan scan;

  public ClientSideScanner() {
    try {
      conf = HBaseUtils.getInstance().getConf();

      connection = ConnectionFactory.createConnection(conf, pool);

      scan = new Scan();
      scan.setBatch(100);
      scan.setCacheBlocks(false);


    } catch (Exception e) {
      log.error(e.getMessage());
    }
  }

  public void tableScan(String table, String cf) throws Exception {
    scan.addFamily(Bytes.toBytes(cf));

    TableName tableName = TableName.valueOf(table);
    Table hTable = connection.getTable(tableName);
    HTableDescriptor htd = hTable.getTableDescriptor();

    Admin hAdmin = connection.getAdmin();
    List<HRegionInfo> hRegionInfoList = hAdmin.getTableRegions(tableName);

    FileSystem fs = FileSystem.get(conf);
    Path root = FSUtils.getRootDir(conf);

    for (HRegionInfo hRegionInfo : hRegionInfoList) {
      regionScan(fs, root, htd, hRegionInfo);
    }

  }

  public void regionScan(FileSystem fs, Path root, HTableDescriptor htd, HRegionInfo hRegionInfo)
      throws Exception {

    ClientSideRegionScanner scanner =
        new ClientSideRegionScanner(
            conf,
            fs,
            root,
            new ReadOnlyTableDescriptor(htd),
            hRegionInfo,
            scan,
            null);

    Result result;
    while ((result = scanner.next()) != null) {
      printScanResult(result);
    }

    scanner.close();

  }

  private void printScanResult(Result result) {
    String rowkey = Bytes.toString(result.getRow());
    for (Cell cell : result.rawCells()) {
      printCell(rowkey, cell);
    }
  }

  private void printCell(String rowKey, Cell cell) {
    String cf = Bytes.toString(CellUtil.cloneFamily(cell));
    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
    String value = Bytes.toString(CellUtil.cloneValue(cell));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    log.debug("Row: {}, {}:{} {}, {}", rowKey, cf, qualifier, value,
        sdf.format(new Date(cell.getTimestamp())));

  }

  public static void main(String[] args) {
    ClientSideScanner hFileScanner = new ClientSideScanner();
    try {
      hFileScanner.tableScan("table_name", "column_family_name");
    } catch (Exception e) {
      e.getMessage();
    }
  }
}
