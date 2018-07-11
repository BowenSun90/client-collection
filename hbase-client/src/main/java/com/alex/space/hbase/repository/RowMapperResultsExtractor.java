package com.alex.space.hbase.repository;

import com.alex.space.hbase.factory.HbaseDataAccessException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * 结果提取器
 *
 * @author Alex Created by Alex on 2018/7/10.
 */
public class RowMapperResultsExtractor<T> implements ResultsExtractor<List<T>> {

  private final RowMapper<T> rowMapper;

  public RowMapperResultsExtractor(RowMapper<T> rowMapper) {
    if (rowMapper == null) {
      throw new HbaseDataAccessException("RowMapper is required");
    }
    this.rowMapper = rowMapper;
  }

  @Override
  public List<T> extractData(ResultScanner results) throws Exception {
    List<T> rs = new ArrayList<>();
    int rowNum = 0;
    for (Result result : results) {
      T t = this.rowMapper.mapRow(result, rowNum++);
      if (t == null) {
        continue;
      }
      rs.add(t);
    }
    return rs;
  }
}
