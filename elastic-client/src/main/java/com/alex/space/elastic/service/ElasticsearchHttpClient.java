package com.alex.space.elastic.service;

import com.alex.space.common.utils.HttpClientUtil;
import com.alex.space.elastic.config.ElasticConstants;
import com.alex.space.elastic.entity.EsCount;
import com.alex.space.elastic.entity.EsDeleteResult;
import com.alex.space.elastic.entity.EsResult;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Elastic search http client
 *
 * @author Alex Created by Alex on 2018/7/10.
 */
@Slf4j
public class ElasticsearchHttpClient {

  private static final String ES_SQL = "_sql";
  private static final String ES_SEARCH = "_search";
  private static final String ES_COUNT = "_count";
  private static final String ES_LIMIT = "limit";

  private static ElasticNodeHostChecker esNodeHostChecker = ElasticNodeHostChecker.getInstance();

  public ElasticsearchHttpClient() {
  }

  public EsResult query(String sql) {
    return query(sql, false);
  }

  public EsResult query(String sql, boolean autoLimit) {
    if (!autoLimit && !sql.contains(ES_LIMIT)) {
      long limit = queryCount(sql);
      sql += " limit " + limit;
    }

    final String sqlString = sql;
    return esNodeHostChecker.doHandle(() -> {
      EsResult esResult =
          HttpClientUtil
              .post("http://" + esNodeHostChecker.getEsNodeHost() + "/" + ES_SQL, sqlString,
                  "UTF-8",
                  EsResult.class);
      return esResult;
    });
  }

  public long queryCount(final String sql) {
    return esNodeHostChecker.doHandle(() -> {
      EsResult esResult =
          HttpClientUtil
              .post("http://" + esNodeHostChecker.getEsNodeHost() + "/" + ES_SQL, sql, "UTF-8",
                  EsResult.class);
      return esResult;
    }).getHits().getTotal();
  }

  public EsResult _search(final String index, final String type, final String searchJson) {
    return esNodeHostChecker.doHandle(() -> {
      EsResult esResult =
          HttpClientUtil.post(
              "http://" + esNodeHostChecker.getEsNodeHost() + "/" + index + "/" + type + "/"
                  + ES_SEARCH,
              searchJson, "UTF-8", EsResult.class);
      return esResult;
    });
  }

  public EsCount _count(final String index, final String type, final String countJson) {
    return esNodeHostChecker.doHandle(() -> {
      EsCount esCount =
          HttpClientUtil.post(
              "http://" + esNodeHostChecker.getEsNodeHost() + "/" + index + "/" + type + "/"
                  + ES_COUNT
              ,
              countJson, EsCount.class);
      return esCount;
    });
  }

  public EsDeleteResult _deleteById(final String index, final String type, final String id) {
    return esNodeHostChecker.doHandle(() -> {
      EsDeleteResult esDeleteResult = HttpClientUtil
          .delete(
              "http://" + esNodeHostChecker.getEsNodeHost() + "/" + index + "/" + type + "/" + id,
              EsDeleteResult.class);
      return esDeleteResult;
    });
  }


  public EsDeleteResult _deleteByQuery(final String index, final String type, final Map param) {
    return esNodeHostChecker.doHandle(() -> {
      EsDeleteResult esDeleteResult =
          HttpClientUtil
              .delete("http://" + esNodeHostChecker.getEsNodeHost() + "/" + index + "/" + type,
                  param,
                  EsDeleteResult.class);
      return esDeleteResult;
    });
  }

  public void insert(final String index, final String type, final Object obj) throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    final String json = objectMapper.writeValueAsString(obj);
    esNodeHostChecker.doHandle(() -> {
      HttpClientUtil
          .postWithJson("http://" + esNodeHostChecker.getEsNodeHost() + "/" + index + "/" + type,
              json);
      return null;
    });
  }

  public void insert(final String index, final String type, final String id, final Object obj)
      throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    final String json = objectMapper.writeValueAsString(obj);
    esNodeHostChecker.doHandle(() -> {
      HttpClientUtil.postWithJson(
          "http://" + esNodeHostChecker.getEsNodeHost() + "/" + index + "/" + type + "/" + id,
          json);
      return null;
    });
  }

  public void batchInsert(String index, String type, List<Map<String, Object>> docList)
      throws Exception {
    StringBuilder sb = new StringBuilder();
    ObjectMapper objectMapper = new ObjectMapper();
    for (Map<String, Object> object : docList) {
      Map<String, Object> createMap = new HashMap<>();
      Map<String, Object> deletMap = new HashMap<>();

      Map<String, Object> idMap = new HashMap<>();
      idMap.put("_index", index);
      idMap.put("_type", type);
      idMap
          .put("_id", URLEncoder.encode(object.get(ElasticConstants.ROWKEY).toString(), "UTF-8"));
      deletMap.put("delete", idMap);
      sb.append(objectMapper.writeValueAsString(deletMap));
      sb.append("\n");

      createMap.put("create", idMap);
      sb.append(objectMapper.writeValueAsString(createMap));
      sb.append("\n");
      sb.append(objectMapper.writeValueAsString(object));
      sb.append("\n");
    }
    final String json = sb.toString();
    esNodeHostChecker.doHandle(() -> {
      HttpClientUtil.postWithJson("http://" + esNodeHostChecker.getEsNodeHost() + "/_bulk", json);
      return null;
    });
  }

  public void batchInsertCommon(String index, String type, List<Map<String, Object>> docList)
      throws Exception {
    StringBuilder sb = new StringBuilder();
    ObjectMapper objectMapper = new ObjectMapper();
    for (Map<String, Object> object : docList) {
      Map<String, Object> createMap = new HashMap<>();
      Map<String, Object> idMap = new HashMap<>();
      idMap.put("_index", index);
      idMap.put("_type", type);
      createMap.put("index", idMap);
      sb.append(objectMapper.writeValueAsString(createMap));
      sb.append("\n");
      sb.append(objectMapper.writeValueAsString(object));
      sb.append("\n");
    }
    final String json = sb.toString();
    esNodeHostChecker.doHandle(() -> {
      HttpClientUtil.postWithJson("http://" + esNodeHostChecker.getEsNodeHost() + "/_bulk", json);
      return null;
    });
  }
}
 