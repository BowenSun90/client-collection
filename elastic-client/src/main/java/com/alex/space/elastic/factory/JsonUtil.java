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

    try {
      XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
      XContentBuilder builder = jsonBuild.startObject();
      for (int i = 0; i < 20; i++) {
        builder.field(generateRandomKey(), generateRandomValue());
      }
      builder.endObject();

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

  private static String generateRandomValue() {
    final char[] chars = "abcde".toCharArray();

    return StringUtils
        .generateStringMessage(ThreadLocalRandom.current().nextInt(2, 8), chars)
        .toLowerCase();
  }

}
