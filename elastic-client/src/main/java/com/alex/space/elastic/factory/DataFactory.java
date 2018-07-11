package com.alex.space.elastic.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Alex Created by Alex on 2018/5/24.
 */
public class DataFactory {

  private static DataFactory dataFactory = new DataFactory();

  private DataFactory() {
  }

  public DataFactory getInstance() {
    return dataFactory;
  }

  public static List<String> getInitJsonData() {
    Random random = new Random();
    List<String> list = new ArrayList<>();
    String data1 = JsonUtil
        .model2Json(new Blog(random.nextInt(100), "git简介", "2016-06-19", "SVN与Git最主要的区别..."));
    String data2 = JsonUtil.model2Json(
        new Blog(random.nextInt(100), "Java中泛型的介绍与简单使用", "2016-06-19", "学习目标 掌握泛型的产生意义..."));
    String data3 = JsonUtil
        .model2Json(new Blog(random.nextInt(100), "SQL基本操作", "2016-06-19", "基本操作：CRUD ..."));
    String data4 = JsonUtil.model2Json(
        new Blog(random.nextInt(100), "Hibernate框架基础", "2016-06-19", "Hibernate框架基础..."));
    String data5 = JsonUtil
        .model2Json(new Blog(random.nextInt(100), "Shell基本知识", "2016-06-19", "Shell是什么..."));
    String data6 = JsonUtil
        .model2Json(new Blog(random.nextInt(100), "HBase权威指南", "2018-03-01", "Hbase是什么..."));
    String data7 = JsonUtil
        .model2Json(new Blog(random.nextInt(100), "Hadoop权威指南", "2017-06-19", "Hadoop是什么..."));

    list.add(data1);
    list.add(data2);
    list.add(data3);
    list.add(data4);
    list.add(data5);
    list.add(data6);
    list.add(data7);
    return list;
  }
}
