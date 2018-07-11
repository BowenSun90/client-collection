package com.alex.space.hbase.repository;

import com.alex.space.hbase.config.HBaseConstants;
import com.alex.space.hbase.factory.HbaseDataAccessException;
import com.alex.space.hbase.utils.HBaseUtils;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase操作模板类
 *
 * @author Alex Created by Alex on 2018/6/22.
 */
public class HBaseRepository extends HBaseAccessor {

  /**
   * @param hbaseZookeeperQuorum hbaseZookeeperQuorum
   */
  public HBaseRepository(String hbaseZookeeperQuorum) {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
    this.setConfiguration(configuration);
    this.afterPropertiesSet();
  }

  /**
   * @param configuration HBaseConfiguration
   */
  public HBaseRepository(Configuration configuration) {
    this.setConfiguration(configuration);
    this.afterPropertiesSet();
  }

  /**
   * @param configuration HBaseConfiguration
   * @param tableFactory HTableInterfaceFactory
   */
  public HBaseRepository(Configuration configuration, HTableInterfaceFactory tableFactory) {
    this.setConfiguration(configuration);
    this.setTableFactory(tableFactory);
    this.afterPropertiesSet();
  }

  public <T> T execute(String tableName, TableCallback<T> action) {
    if (action == null) {
      throw new HbaseDataAccessException("Callback object must not be null");
    }
    if (tableName == null) {
      throw new HbaseDataAccessException("No table specified");
    }

    HTableInterface table = getTable(tableName);

    try {
      boolean previousFlushSetting = applyFlushSetting(table);
      T result = action.doInTable(table);
      flushIfNecessary(table, previousFlushSetting);
      return result;
    } catch (Throwable th) {
      if (th instanceof Error) {
        throw ((Error) th);
      }
      if (th instanceof RuntimeException) {
        throw ((RuntimeException) th);
      }
      throw new HbaseDataAccessException((Exception) th);
    } finally {
      releaseTable(tableName, table);
    }
  }

  private HTableInterface getTable(String tableName) {
    return HBaseUtils.getHTable(tableName, getConfiguration(), getCharset(), getTableFactory());
  }

  private void releaseTable(String tableName, HTableInterface table) {
    HBaseUtils.releaseTable(tableName, table, getTableFactory());
  }

  private void restoreFlushSettings(HTableInterface table, boolean oldFlush) {
    if (table instanceof HTable) {
      if (table.isAutoFlush() != oldFlush) {
        table.setAutoFlush(oldFlush);
      }
    }
  }

  private void flushIfNecessary(HTableInterface table, boolean oldFlush) throws IOException {
    // TODO: check whether we can consider or not a table scope
    table.flushCommits();
    restoreFlushSettings(table, oldFlush);
  }

  private boolean applyFlushSetting(HTableInterface table) {
    boolean autoFlush = table.isAutoFlush();
    if (table instanceof HTable) {
      table.setAutoFlush(true);
    }
    return autoFlush;
  }

  public <T> T find(String tableName, String family, final ResultsExtractor<T> action) {
    Scan scan = new Scan();
    scan.addFamily(family.getBytes(getCharset()));
    return find(tableName, scan, action);
  }

  public <T> T find(String tableName, String family, String qualifier,
      final ResultsExtractor<T> action) {
    Scan scan = new Scan();
    scan.addColumn(family.getBytes(getCharset()), qualifier.getBytes(getCharset()));
    return find(tableName, scan, action);
  }

  public <T> T find(String tableName, final Scan scan, final ResultsExtractor<T> action) {
    return execute(tableName, htable -> {
      try (ResultScanner scanner = htable.getScanner(scan)) {
        return action.extractData(scanner);
      }
    });
  }

  public <T> List<T> find(String tableName, String family, final RowMapper<T> action) {
    Scan scan = new Scan();
    scan.addFamily(family.getBytes(getCharset()));
    return find(tableName, scan, action);
  }

  public <T> List<T> find(String tableName, String family, String qualifier,
      final RowMapper<T> action) {
    Scan scan = new Scan();
    scan.addColumn(family.getBytes(getCharset()), qualifier.getBytes(getCharset()));
    return find(tableName, scan, action);
  }

  public <T> T get(String tableName, String rowName, final RowMapper<T> mapper) {
    return get(tableName, rowName, null, null, mapper);
  }

  public <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper) {
    return get(tableName, rowName, familyName, null, mapper);
  }

  public <T> T get(String tableName, final String rowName, final String familyName,
      final String qualifier, final RowMapper<T> mapper) {
    return execute(tableName, htable -> {
      Get get = new Get(rowName.getBytes(getCharset()));
      if (familyName != null) {
        byte[] family = familyName.getBytes(getCharset());

        if (qualifier != null) {
          get.addColumn(family, qualifier.getBytes(getCharset()));
        } else {
          get.addFamily(family);
        }
      }
      Result result = htable.get(get);
      return mapper.mapRow(result, 0);
    });
  }

  public void put(String tableName, final String rowName, final String familyName,
      final String qualifier, final byte[] value) {

    if (rowName == null || "".equals(rowName) || familyName == null || "".equals(familyName)
        || qualifier == null || ""
        .equals(qualifier)
        || value == null || "".equals(value)) {
      throw new HbaseDataAccessException("");
    }

    execute(tableName, htable -> {
      Put put = new Put(rowName.getBytes(getCharset()))
          .add(familyName.getBytes(getCharset()), qualifier.getBytes(getCharset()), value);
      htable.put(put);
      return null;
    });
  }

  public void delete(String tableName, final String rowName, final String familyName) {
    delete(tableName, rowName, familyName, null);
  }

  public void delete(String tableName, final String rowName, final String familyName,
      final String qualifier) {

    execute(tableName, htable -> {
      Delete delete = new Delete(rowName.getBytes(getCharset()));
      byte[] family = familyName.getBytes(getCharset());

      if (qualifier != null) {
        delete.deleteColumn(family, qualifier.getBytes(getCharset()));
      } else {
        delete.deleteFamily(family);
      }

      htable.delete(delete);
      return null;
    });
  }

  public void delete(final String tableName, final String rowKey) {

    execute(tableName, htable -> {
      byte[] rowkey = Bytes.toBytes(rowKey);
      if (!htable.exists(new Get(rowkey))) {
        throw new Exception("需要删除的rowKey=" + rowKey + "记录不存在,无法进行删除操作");
      }
      Delete delete = new Delete(rowkey);
      htable.delete(delete);
      return null;
    });
  }

  public <T> T get(String tableName, byte[] rowName, final RowMapper<T> mapper) {
    return get(tableName, rowName, null, null, mapper);
  }

  public <T> T get(String tableName, final byte[] rowName, final String familyName,
      final String qualifier, final RowMapper<T> mapper) {
    return execute(tableName, htable -> {
      Get get = new Get(rowName);
      get.addFamily(HBaseConstants.HBASE_DEFAULT_FAMILY);
      if (familyName != null) {
        byte[] family = familyName.getBytes(getCharset());

        if (qualifier != null) {
          get.addColumn(family, qualifier.getBytes(getCharset()));
        } else {
          get.addFamily(family);
        }
      }
      Result result = htable.get(get);
      return mapper.mapRow(result, 0);
    });
  }

  public <T> List<T> find(String tableName, final Scan scan, int offset,
      final RowMapper<T> action) {
    scan.addFamily(HBaseConstants.HBASE_DEFAULT_FAMILY);
    return find(tableName, scan, new OffsetMapperResultsExtractor<T>(action, offset));
  }

  public <T> T queryCount(String tableName, Scan scan) {
    return execute(tableName, table -> {
      Scan scan1 = new Scan();
      scan1.addFamily(HBaseConstants.HBASE_DEFAULT_FAMILY);
      scan1.setCaching(500);
      ResultScanner rs = table.getScanner(scan1);
      Long count = 0L;

      for (Result res : rs) {
        count++;
      }
      return (T) count;
    });
  }

  public <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action) {
    scan.addFamily(HBaseConstants.HBASE_DEFAULT_FAMILY);
    return find(tableName, scan, new RowMapperResultsExtractor<T>(action));
  }
}
