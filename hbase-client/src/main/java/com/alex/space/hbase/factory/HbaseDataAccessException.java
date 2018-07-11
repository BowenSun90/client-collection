package com.alex.space.hbase.factory;

/**
 * HBase Data Access exception.
 *
 * @author alex Created by Alex on 2018/6/3.
 */
public class HbaseDataAccessException extends RuntimeException {


  public HbaseDataAccessException(String message) {
    super(message);
  }

  public HbaseDataAccessException(Exception cause) {
    super(cause.getMessage(), cause);
  }
}
