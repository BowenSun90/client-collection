package com.alex.space.hbase.repository;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * Callback for mapping rows of a {@link ResultScanner} on a per-row basis. Implementations of this
 * interface perform the actual work of mapping each row to a result object, but don't need to worry
 * about exception handling.
 *
 * @author Costin Leau
 */
public interface RowMapper<T> {

  /**
   * Row mapper function
   *
   * @param result Row
   * @param rowNum row num
   * @return MapperResult
   * @throws Exception exception
   */
  T mapRow(Result result, int rowNum) throws Exception;
}
