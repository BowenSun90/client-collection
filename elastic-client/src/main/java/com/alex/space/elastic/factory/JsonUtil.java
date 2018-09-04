package com.alex.space.elastic.factory;

import com.alex.space.common.utils.StringUtils;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * @author Alex Created by Alex on 2018/5/24.
 */
@Slf4j
public class JsonUtil {

  /**
   * Convert java obj (Blog) to json format
   */
  public static String model2Json(Blog blog) {
    String jsonData = null;
    try {
      XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
      jsonBuild.startObject()
          .field("id", blog.getId())
          .field("title", blog.getTitle())
          .field("post", blog.getPostTime())
          .field("content", blog.getContent())
          .endObject();

      jsonData = jsonBuild.string();

      log.debug(jsonData);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return jsonData;
  }

  public static String randomJson() {
    String jsonData = null;

    String key1 = generateRandomKey();
    String key2 = generateRandomKey();
    String key3 = generateRandomKey();

    String value1 = generateRandomKey();
    String value2 = generateRandomKey();
    String value3 = generateRandomKey();

    try {
      XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
      jsonBuild.startObject()
          .field(key1, value1)
          .field(key2, value2)
          .field(key3, value3)
          .endObject();

      jsonData = jsonBuild.string();

      log.debug(jsonData);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return jsonData;
  }

  private static String generateRandomKey() {
    final char[] chars = "bcde".toCharArray();

    return StringUtils
        .generateStringMessage(ThreadLocalRandom.current().nextInt(2, 5), chars)
        .toLowerCase();
  }

}
