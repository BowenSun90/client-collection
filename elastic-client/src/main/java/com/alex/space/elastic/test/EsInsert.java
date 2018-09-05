package com.alex.space.elastic.test;

import com.alex.space.elastic.factory.DataFactory;
import com.alex.space.elastic.utils.ElasticUtils;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.StopWatch;
import org.joda.time.PeriodType;

/**
 * 插入测试数据
 *
 * @author Alex Created by Alex on 2018/9/4.
 */
@Slf4j
public class EsInsert {

  public static void main(String[] args) throws UnknownHostException {
    ElasticUtils.init();

    insertData();

    updateData();

    queryData();

  }

  /**
   * 插入稀疏数据测试
   */
  private static void insertSparseData() {
    String index = "test";
    String type = "data";

    try {

      StopWatch stopWatch = new StopWatch();
      stopWatch.start();

      for (int i = 0; i < 100; i++) {
        List<String> jsonData = DataFactory.getInitRandomJsonData();

        ElasticUtils.insertData(index, type, jsonData);

        if (i % 100 == 0 && i != 0) {
          stopWatch.stop();
          log.info(Thread.currentThread().getName() + "Insert Time: " + stopWatch.totalTime()
              .format(PeriodType.millis()));
          stopWatch = new StopWatch();
          stopWatch.start();
        }

      }

    } catch (Exception e) {
      log.error(e.getMessage());
    }

  }

  private static void updateRandomData() {
    String index = "test";
    String type = "data";

    try {

      StopWatch stopWatch = new StopWatch();
      stopWatch.start();

      for (int j = 0; j < 10000; j++) {
        ElasticUtils.updateData(index, type);
        if (j % 1000 == 0 && j != 0) {
          stopWatch.stop();
          log.info(Thread.currentThread().getName() + "Update Time: " + stopWatch.totalTime()
              .format(PeriodType.millis()));
          stopWatch = new StopWatch();
          stopWatch.start();
        }
      }

    } catch (Exception e) {
      log.error(e.getMessage());
    }

  }

  /**
   * 多线程插入数据测试
   */
  private static void insertData() {

    ExecutorService pool = Executors.newFixedThreadPool(5);
    for (int i = 0; i < 100; i++) {
      pool.submit(EsInsert::insertSparseData);
    }

    pool.shutdown();
  }

  private static void updateData() {

    ExecutorService pool = Executors.newFixedThreadPool(5);
    for (int i = 0; i < 100; i++) {
      pool.submit(EsInsert::updateRandomData);
    }

    pool.shutdown();
  }

  private static void queryData() {
    String index = "test";
    String type = "data";

    ExecutorService pool = Executors.newFixedThreadPool(5);
    for (int i = 0; i < 1000; i++) {
      pool.submit(() -> ElasticUtils.queryData(index, type));

    }

    pool.shutdown();
  }

}
