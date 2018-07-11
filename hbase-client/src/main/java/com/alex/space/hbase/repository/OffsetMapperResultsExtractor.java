package com.alex.space.hbase.repository;

import com.alex.space.hbase.factory.HbaseDataAccessException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * 支持带偏移量的结果提取器
 *
 * @author Alex Created by Alex on 2018/7/10.
 */
public class OffsetMapperResultsExtractor<T> implements ResultsExtractor<List<T>> {

  private final RowMapper<T> rowMapper;

  /**
   * 偏移量
   */
  private Integer offset;

  public OffsetMapperResultsExtractor(RowMapper<T> rowMapper, Integer offset) {
    if (rowMapper == null) {
      throw new HbaseDataAccessException("RowMapper is required");
    }
    if (offset == null) {
      throw new HbaseDataAccessException("Offset is required");
    }
    this.rowMapper = rowMapper;
    this.offset = offset;
  }

  @Override
  public List<T> extractData(ResultScanner results) throws Exception {
    List<T> rs = new ArrayList<T>();
    int rowNum = 0;
    for (Result result : results) {
      rowNum++;
      if (rowNum < offset) {
        continue;
      }
      rs.add(this.rowMapper.mapRow(result, rowNum));
    }
    return rs;
  }

}
