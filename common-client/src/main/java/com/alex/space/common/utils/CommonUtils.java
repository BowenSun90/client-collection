package com.alex.space.common.utils;

import java.io.Closeable;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/**
 * Common Utils
 *
 * @author Alex Created by Alex on 2018/6/6.
 */
@Slf4j
public class CommonUtils {

  /**
   * close obj with closeable
   *
   * @param obj object with closeable
   */
  public static void close(Closeable... obj) {
    for (Closeable c : obj) {
      try {
        if (c != null) {
          c.close();
        }
      } catch (IOException e) {
        log.error("close " + c.toString() + " with exception,", e);
      }
    }

  }
}
