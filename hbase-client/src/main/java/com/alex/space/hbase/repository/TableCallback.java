package com.alex.space.hbase.repository;

import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Callback interface for Hbase code. To be used with execution methods, often as anonymous classes
 * within a method implementation without having to worry about exception handling.
 *
 * @author Costin Leau
 */
public interface TableCallback<T> {

  /**
   * Gets called by execute with an active Hbase table. Does need to care about activating or
   * closing down the table.
   *
   * @param table active Hbase table
   * @return a result object, or null if none
   * @throws Throwable thrown by the Hbase API
   */
  T doInTable(HTableInterface table) throws Throwable;
}
