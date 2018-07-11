package com.alex.space.elastic.factory;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Alex Created by Alex on 2018/5/24.
 */
@Getter
@Setter
public class Blog {

  private Integer id;

  private String title;

  private String postTime;

  private String content;

  public Blog(Integer id, String title, String postTime, String content) {
    this.id = id;
    this.title = title;
    this.postTime = postTime;
    this.content = content;
  }

}