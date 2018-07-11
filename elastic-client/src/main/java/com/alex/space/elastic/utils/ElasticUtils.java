package com.alex.space.elastic.utils;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import com.alex.space.elastic.config.ElasticConfig;
import com.alex.space.elastic.factory.DataFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Elastic Utils
 *
 * @author Alex Created by Alex on 2018/5/24.
 */
@Slf4j
public class ElasticUtils {

  private static ElasticConfig elasticConfig = ElasticConfig.getInstance();

  private static TransportClient client;

  private static void init() throws UnknownHostException {

    //设置集群名称
    Settings settings = Settings.builder()
        .put("cluster.name", elasticConfig.getProperty("es.cluster.name")).build();

    //创建client
    client = new PreBuiltTransportClient(settings).addTransportAddress(
        new InetSocketTransportAddress(
            InetAddress.getByName(elasticConfig.getProperty("es.node.ip")),
            elasticConfig.getIntProperty("es.node.port")));
  }

  /**
   * 测试集群
   */
  private static void testClient() {
    //搜索数据
    GetResponse response = client.prepareGet("accounts", "person", "2").execute().actionGet();

    //输出结果
    System.out.println(response.getSourceAsString());
  }

  private static void createIndex() throws IOException {
    IndexResponse response = client.prepareIndex("twitter", "tweet", "2")
        .setSource(
            jsonBuilder()
                .startObject()
                .field("user", "alex")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject()
        )
        .get();

    System.out.println(response);
  }

  /**
   * Insert index into elastic cluster
   *
   * @param index index name
   * @param type type name
   * @param records record list, json format
   */
  private static void insertData(String index, String type, List<String> records) {

    Random random = new Random();
    for (String jsonData : records) {
      IndexResponse response = client.prepareIndex(index, type, String.valueOf(random.nextInt(100)))
          .setSource(jsonData, XContentType.JSON)
          .get();
      System.out.println("Insert:\t" + response);
    }
  }

  /**
   * Query data by matching column value
   *
   * @param index index name
   * @param type type name
   * @param value column value
   * @param fields field list
   */
  private static void queryData(String index, String type, String value, String... fields) {

    QueryBuilder query = QueryBuilders.multiMatchQuery(value, fields);

    SearchResponse response = client.prepareSearch(index)
        .setTypes(type)
        .setQuery(query)
        .execute()
        .actionGet();

    SearchHits hits = response.getHits();
    if (hits.totalHits > 0) {
      for (SearchHit hit : hits) {
        log.info("score:" + hit.getScore() + ":\t" + hit.getSource());
      }
    } else {
      log.info("搜到0条结果");
    }

  }

  /**
   * Update index value
   */
  private static void updateData() throws IOException, ExecutionException, InterruptedException {
    // 更新方式一 创建一个UpdateRequest,然后将其发送给client
    UpdateRequest uRequest = new UpdateRequest();
    uRequest.index("blog");
    uRequest.type("article");
    uRequest.id("7");
    uRequest.doc(
        jsonBuilder()
            .startObject()
            .field("content", "学习目标 掌握java泛型的产生意义ssss")
            .endObject()
    );
    UpdateResponse response = client.update(uRequest).get();
    System.out.println("Update:\t" + response);

    // 更新方式二 prepareUpdate() 使用doc更新索引
    response = client.prepareUpdate("blog", "article", "18")
        .setDoc(
            jsonBuilder()
                .startObject()
                .field("content", "SVN与Git对比。。。")
                .endObject()
        ).get();
    System.out.println("Update:\t" + response);

    // 更新方式三 增加新的字段
    UpdateRequest updateRequest = new UpdateRequest("blog", "article", "88")
        .doc(jsonBuilder().startObject().field("comment", "comment").endObject());
    client.update(updateRequest).get();
    System.out.println("Update:\t" + updateRequest);

    // 更新方式四 upsert 如果文档不存在则创建新的索引
    IndexRequest indexRequest = new IndexRequest("blog", "article", "103")
        .source(jsonBuilder().startObject()
            .field("title", "Git安装10")
            .field("content", "学习目标 git。。。10")
            .endObject());
    System.out.println("Update:\t" + indexRequest);

    UpdateRequest uRequest2 = new UpdateRequest("blog", "article", "104")
        .doc(jsonBuilder().startObject()
            .field("title", "Git安装")
            .field("content", "学习目标 git。。。")
            .endObject())
        .upsert(indexRequest);
    client.update(uRequest2).get();
    System.out.println("Update:\t" + uRequest2);

  }

  private static void deleteIndex(String indexName) {

    // 判断索引是否存在
    IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexName);
    IndicesExistsResponse inExistsResponse = client.admin().indices().exists(inExistsRequest)
        .actionGet();
    if (inExistsResponse.isExists()) {
      System.out.println("Index: " + indexName + " exists.");

      DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(indexName).execute()
          .actionGet();
      if (dResponse.isAcknowledged()) {
        System.out.println("Index: " + indexName + " delete success.");
      } else {
        System.out.println("Index: " + indexName + " delete failed.");
      }
    } else {
      System.out.println("Index: " + indexName + " not exists.");
    }
  }

  private static void deleteDocById(String indexName, String type, String id) {
    DeleteResponse dResponse = client.prepareDelete(indexName, type, id).execute().actionGet();

    System.out.println(dResponse);
    if (dResponse.getResult().getLowercase().equalsIgnoreCase("deleted")) {
      System.out
          .println("Index: " + indexName + ", Type: " + type + "Id: " + id + " delete success.");
    } else {
      System.out
          .println("Index: " + indexName + ", Type: " + type + "Id: " + id + " delete failed.");
    }
  }

  private static void export() throws IOException {
    QueryBuilder qb = QueryBuilders.matchAllQuery();

    SearchResponse response = client.prepareSearch("blog")
        .setTypes("article")
        .setQuery(QueryBuilders.matchAllQuery())
        .execute().actionGet();

    SearchHits resultHits = response.getHits();
    File article = new File("bulk.txt");
    FileWriter fw = new FileWriter(article);
    BufferedWriter bfw = new BufferedWriter(fw);

    if (resultHits.getHits().length == 0) {
      System.out.println("查到0条数据!");

    } else {
      for (int i = 0; i < resultHits.getHits().length; i++) {
        String jsonStr = resultHits.getHits()[i]
            .getSourceAsString();
        System.out.println(jsonStr);
        bfw.write(jsonStr);
        bfw.write("\n");
      }
    }

    bfw.close();
    fw.close();
  }

  private static void bulkIn() throws IOException {
    File article = new File("bulk.txt");
    FileReader fr = new FileReader(article);
    BufferedReader bfr = new BufferedReader(fr);
    String line = null;
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    int count = 0;
    while ((line = bfr.readLine()) != null) {
      bulkRequest.add(client.prepareIndex("test", "article").setSource(line));
      if (count % 10 == 0) {
        bulkRequest.execute().actionGet();
      }
      count++;
    }
    bulkRequest.execute().actionGet();

    bfr.close();
    fr.close();
  }

  /**
   * Elastic API Test
   */
  public static void main(String[] args) {

    String index = "blog";
    String type = "article";

    try {
      init();
      List<String> jsonData = DataFactory.getInitJsonData();

      insertData(index, type, jsonData);

      queryData(index, type, "git", "title", "content");

      updateData();

      deleteIndex("test");

      deleteDocById("blog", "article", "1");

      deleteDocById("blog", "article", "2");

      export();

      bulkIn();

      System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    } finally {
      // 关闭client
      if (client != null) {
        client.close();
      }
    }

  }

}