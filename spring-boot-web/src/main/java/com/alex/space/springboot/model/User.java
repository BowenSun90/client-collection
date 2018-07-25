package com.alex.space.springboot.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Application
 *
 * @author Alex Created by Alex on 2017/06/01
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User implements Serializable {

  private static final long serialVersionUID = 1L;

  @JsonProperty("name")
  private String name;

  @JsonProperty("code")
  private int code;

  @JsonIgnore
  private String city;

}
