package com.alex.space.zoo.entity;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * User entity, need to implement Serializable
 *
 * @author Alex Created by Alex on 2018/6/19.
 */
@Setter
@Getter
@ToString
public class User implements Serializable {

  private Integer id;

  private String name;
}
