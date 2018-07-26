package com.alex.space.hbase.config;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase Constants
 *
 * @author Alex Created by Alex on 2018/6/4.
 */
public class HBaseConstants {

  /**
   * Default column family name
   */
  public static final String HBASE_DEFAULT_FAMILY_NAME = "cf";

  public static final byte[] HBASE_DEFAULT_FAMILY = Bytes.toBytes(HBASE_DEFAULT_FAMILY_NAME);

  public static final long MEMSTORE_FLUSH_SIZE = 256 * 1024 * 1024;

  public static final boolean COMPACTION_ENABLED = true;

  public interface MapRedConstants {

    String INPUT_TABLE_NAME = "table1";

    String OUTPUT_TABLE_NAME = "table2";

    byte[] FAMILY_NAME = Bytes.toBytes("cf");

    byte[] QUALIFIER_NAME = Bytes.toBytes("col1");

  }
}
