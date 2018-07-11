package com.alex.space.hbase.factory;

import com.alex.space.hadoop.hdfs.HDFS;
import com.alex.space.hbase.config.HBaseConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * HBase Helper to read configuration xml
 *
 * @author Alex Created by Alex on 2018/4/6.
 */
@Slf4j
public class HbaseHelper {

  private static HbaseHelper instance = null;

  public static synchronized HbaseHelper getInstance() {
    if (instance == null) {
      instance = new HbaseHelper();
    }
    return instance;
  }

  private static HBaseConfig hBaseConfig = HBaseConfig.getInstance();

  private org.apache.hadoop.conf.Configuration configuration;

  private PooledHTableInterfaceFactory tableFactory;

  /**
   * Get HTableInterfaceFactory
   */
  public synchronized PooledHTableInterfaceFactory getTableFactory() {
    if (tableFactory == null) {
      tableFactory = new PooledHTableInterfaceFactory(getConfiguration());
    }
    return tableFactory;
  }

  /**
   * Get hadoop configuration xml
   */
  public synchronized org.apache.hadoop.conf.Configuration getConfiguration() {
    if (configuration == null) {
      configuration = HBaseConfiguration.create();
      loadConfiguration(configuration);
    }

    return configuration;
  }

  /**
   * Load xml configuration
   *
   * @param configuration hadoop configuration
   */
  private static void loadConfiguration(org.apache.hadoop.conf.Configuration configuration) {

    String hadoopConfPath = hBaseConfig.getProperty("hadoop.conf.path");
    String hbaseConfPath = hBaseConfig.getProperty("hbase.conf.path");

    configuration.addResource(new Path(hadoopConfPath + "/hdfs-site.xml"));
    configuration.addResource(new Path(hadoopConfPath + "/core-site.xml"));
    configuration.addResource(new Path(hadoopConfPath + "/mapred-site.xml"));
    configuration.addResource(new Path(hadoopConfPath + "/yarn-site.xml"));
    configuration.addResource(new Path(hbaseConfPath + "/hbase-site.xml"));

    if (!HDFS.login(configuration)) {
      log.error("Kerberos auth with exception.");
    }
  }

}
