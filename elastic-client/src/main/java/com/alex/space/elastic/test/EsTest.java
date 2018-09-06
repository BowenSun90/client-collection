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
public class EsTest {

  public static void main(String[] args) throws UnknownHostException, InterruptedException {
    ElasticUtils.init();

//    insertData();

//    updateData();

//    queryData();

//    bulkIn();

//    bulkUpdate();

    Thread bulkIn = new Thread(() -> bulkIn());
    bulkIn.start();

    Thread bulkUpdate = new Thread(() -> bulkUpdate());
    bulkUpdate.start();

    while (bulkIn.isAlive() || bulkUpdate.isAlive()) {
      Thread.sleep(30000);
    }

    System.exit(0);
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

      for (int i = 0; i < 1000; i++) {
        List<String> jsonData = DataFactory.getInitRandomJsonData(10);

        ElasticUtils.insertData(index, type, jsonData);

        if (i % 100 == 0 && i != 0) {
          stopWatch.stop();
          log.info(Thread.currentThread().getName() + " Insert Time: " + stopWatch.totalTime()
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
   * 随机更新数据
   */
  private static void updateRandomData() {
    String index = "test";
    String type = "data";

    try {

      StopWatch stopWatch = new StopWatch();
      stopWatch.start();

      for (int j = 0; j < 10000; j++) {
        ElasticUtils.updateData(index, type);
        if (j % 5000 == 0 && j != 0) {
          stopWatch.stop();
          log.info(Thread.currentThread().getName() + " Update Time: " + stopWatch.totalTime()
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

    ExecutorService pool = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      pool.submit(EsTest::insertSparseData);
    }

    pool.shutdown();
  }

  /**
   * 多线程更新数据
   */
  private static void updateData() {

    ExecutorService pool = Executors.newFixedThreadPool(20);
    for (int i = 0; i < 100; i++) {
      pool.submit(EsTest::updateRandomData);
    }

    pool.shutdown();
  }

  /**
   * 多线程查询数据
   */
  private static void queryData() {
    String index = "test";
    String type = "data";

    ExecutorService pool = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10000; i++) {
      pool.submit(() -> ElasticUtils.queryData(index, type));

    }

    pool.shutdown();
  }

  private static void bulkIn() {
    String index = "test";
    String type = "data";

    StopWatch stopWatch = new StopWatch();

    int avg = 0;
    int offset = 4747455;
    for (int i = 0; i < 500; i++) {
      List<String> jsonData = DataFactory.getInitRandomJsonData(5000);
      stopWatch.start();
      ElasticUtils.bulkInsert(index, type, jsonData, offset);
      offset += jsonData.size();
      stopWatch.stop();
      log.debug("Insert offset: " + offset + ", time: " + stopWatch.totalTime().getMillis());
      avg += stopWatch.totalTime().getMillis();
      if (i % 20 == 0 && i != 0) {
        log.info("Avg insert 5000 time:" + avg / 20.0 / 1000.0 + "s.");
        avg = 0;
      }
      stopWatch = new StopWatch();

    }
  }

  private static void bulkUpdate() {
    String index = "test";
    String type = "data";

    StopWatch stopWatch = new StopWatch();

    int avg = 0;
    int offset = 1000000;
    for (int i = 0; i < 500; i++) {
      List<String> jsonData = DataFactory.getInitRandomJsonData(5000);
      stopWatch.start();
      ElasticUtils.bulkUpdate(index, type, jsonData, offset);
      offset += jsonData.size();
      stopWatch.stop();
      log.debug("Update offset: " + offset + ", time: " + stopWatch.totalTime().getMillis());
      avg += stopWatch.totalTime().getMillis();
      if (i % 20 == 0 && i != 0) {
        log.info("Avg update 5000 time:" + avg / 20.0 / 1000.0 + "s.");
        avg = 0;
      }
      stopWatch = new StopWatch();

    }

  }
}
