package com.alex.space.springboot.pojo;

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
@AllArgsConstructor
@NoArgsConstructor
public class Response implements Serializable {

  private static final long serialVersionUID = 1894676594807555661L;

  private int code;

  private String message;

  private Object data;

  public static Response success() {
    return success(null);
  }

  public static Response success(Object data) {
    return new Response(Status.SUCCESS.getCode(), "成功", data);
  }

  public static Response fail() {
    return fail(null);
  }

  public static Response fail(String message) {
    return new Response(999, "失败", message);
  }

  public static Response valueOf(int code, String msg) {
    return new Response(code, msg, null);
  }

  public static Response valueOf(Status status) {
    return valueOf(status.getCode(), status.getMsg());
  }

  public static Response valueOf(Object data) {
    return success(data);
  }

  public enum Status {
    SUCCESS(200, "成功"),

    E400_BAD_REQUEST(400, "参数错误"),

    E404_NOT_FOUND(404, "资源不存在");

    private int code;

    private String msg;

    Status(int code, String msg) {
      this.code = code;
      this.msg = msg;
    }

    public int getCode() {
      return this.code;
    }

    public String getMsg() {
      return this.msg;
    }
  }

}
