package com.alex.space.elastic.factory;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * @author Alex Created by Alex on 2018/5/24.
 */
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

      System.out.println(jsonData);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return jsonData;
  }
}
