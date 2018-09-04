package com.alex.space.elastic.test;

import com.alex.space.elastic.factory.DataFactory;
import com.alex.space.elastic.utils.ElasticUtils;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * 插入测试数据
 *
 * @author Alex Created by Alex on 2018/9/4.
 */
@Slf4j
public class EsInsert {

  public static void main(String[] args) {
    insertSparseData();
  }

  /**
   * 插入稀疏数据测试
   */
  private static void insertSparseData() {
    String index = "test";
    String type = "data";

    try {

      ElasticUtils.init();

      for (int i = 0; i < 100; i++) {
        List<String> jsonData = DataFactory.getInitRandomJsonData();
        ElasticUtils.insertData(index, type, jsonData);
      }

      for (int i = 0; i < 10000; i++) {
        ElasticUtils.updateData(index, type);
      }

    } catch (Exception e) {
      log.error(e.getMessage());
    }
  }


}
