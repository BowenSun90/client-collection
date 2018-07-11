package com.alex.space.hbase.factory;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;

/**
 * Implement of HTableInterfaceFactory
 *
 * @author Alex Created by Alex on 2018/6/3.
 */
public class PooledHTableInterfaceFactory implements HTableInterfaceFactory {

  private Configuration configuration;

  private HConnection hConnection;

  public PooledHTableInterfaceFactory(Configuration configuration) {
    this.configuration = configuration;
    afterPropertiesSet();
  }

  public void afterPropertiesSet() {
    if (configuration == null) {
      throw new HbaseDataAccessException("A valid configuration is required");
    }

    try {
      hConnection = HConnectionManager.createConnection(configuration);
    } catch (IOException e) {
      throw new HbaseDataAccessException(e);
    }
  }

  @Override
  public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
    HTableInterface tableInterface;
    try {
      tableInterface = hConnection.getTable(tableName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return tableInterface;
  }

  @Override
  public void releaseHTableInterface(HTableInterface table) {
    // do nothing
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  public HConnection getHConnection() {
    return hConnection;
  }

}
