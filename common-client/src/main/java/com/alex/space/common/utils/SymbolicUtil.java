package com.alex.space.common.utils;

import java.util.Calendar;
import java.util.Date;

/**
 * 星座、生肖工具类
 *
 * Source: [https://blog.csdn.net/saindy5828/article/details/47416199]
 *
 * @author Alex Created by Alex on 2018/10/16.
 */
public class SymbolicUtil {

  private static final String[] SYMBOLIC_ARRAY = {"猴", "鸡", "狗", "猪", "鼠", "牛",
      "虎", "兔", "龙", "蛇", "马", "羊"};

  private static final String[] ZODIAC_ARRAY = {"水瓶座", "双鱼座", "牡羊座",
      "金牛座", "双子座", "巨蟹座", "狮子座", "处女座", "天秤座", "天蝎座", "射手座", "魔羯座"};

  private static final int[] ZODIAC_EDGE_DAY = {20, 19, 21, 21, 21, 22,
      23, 23, 23, 23, 22, 22};

  /**
   * 根据日期获取生肖
   */
  private static String date2Symbolic(Calendar time) {
    return SYMBOLIC_ARRAY[time.get(Calendar.YEAR) % 12];
  }

  /**
   * 根据日期获取生肖
   */
  public static String date2Symbolic(Date date) {
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    return date2Symbolic(c);
  }

  /**
   * 根据日期获取星座
   */
  private static String date2Zodiac(Calendar time) {
    int month = time.get(Calendar.MONTH);
    int day = time.get(Calendar.DAY_OF_MONTH);
    if (day < ZODIAC_EDGE_DAY[month]) {
      month = month - 1;
    }

    return ZODIAC_ARRAY[month];
  }

  /**
   * 根据日期获取星座
   */
  public static String date2Zodiac(Date date) {
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    return date2Zodiac(c);
  }

}
