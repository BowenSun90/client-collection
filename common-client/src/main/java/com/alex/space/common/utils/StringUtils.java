package com.alex.space.common.utils;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Alex Created by Alex on 2018/8/22.
 */
public class StringUtils {

  private static char[] CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890"
      .toCharArray();

  public static boolean isEmpty(String str) {
    return str == null || "".equals(str);
  }

  /**
   * 产生一个给定长度的字符串
   */
  public static String generateStringMessage(int numItems) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numItems; i++) {
      sb.append(
          CHARS[ThreadLocalRandom.current().nextInt(CHARS.length)]);
    }
    return sb.toString();
  }
}
