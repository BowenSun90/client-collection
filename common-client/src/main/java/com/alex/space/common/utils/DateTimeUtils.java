package com.alex.space.common.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Date Time Utils
 *
 * @author Alex Created by Alex on 2018/9/14.
 */
public class DateTimeUtils {

  /**
   * 获取上一年最后一天
   *
   * @param offset 相对于目标时间偏移量(单位：天)，负数表示向前推移，正数表示向后推移
   * @return Date
   */
  public static Date getLastYearEndDay(Date date, int offset) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    cal.add(Calendar.YEAR, -1);
    int year = cal.get(Calendar.YEAR);
    cal.clear();

    cal.set(Calendar.YEAR, year);
    cal.roll(Calendar.DAY_OF_YEAR, -1);

    cal.add(Calendar.DATE, offset);

    return cal.getTime();
  }

  /**
   * 获取上一月最后一天
   *
   * @param offset 相对于目标时间偏移量(单位：天)，负数表示向前推移，正数表示向后推移
   * @return Date
   */
  public static Date getLastMonthEndDay(Date date, int offset) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    cal.add(Calendar.MONTH, -1);
    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH);
    cal.clear();

    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month);
    cal.roll(Calendar.DAY_OF_MONTH, -1);

    cal.add(Calendar.DATE, offset);

    return cal.getTime();
  }

  /**
   * 获取上一季度最后一天
   *
   * @param offset 相对于目标时间偏移量(单位：天)，负数表示向前推移，正数表示向后推移
   * @return Date
   */
  public static Date getLastQuarterEndDay(Date date, int offset) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    int currentMonth = cal.get(Calendar.MONTH) + 1;
    if (currentMonth >= 1 && currentMonth <= 3) {
      cal.set(Calendar.MONTH, 9);
    } else if (currentMonth >= 4 && currentMonth <= 6) {
      cal.set(Calendar.MONTH, 0);
    } else if (currentMonth >= 7 && currentMonth <= 9) {
      cal.set(Calendar.MONTH, 3);
    } else if (currentMonth >= 10 && currentMonth <= 12) {
      cal.set(Calendar.MONTH, 6);
    }
    cal.set(Calendar.DATE, 0);

    cal.add(Calendar.DATE, offset);

    return cal.getTime();
  }

  /**
   * 获取上一周最后一天
   *
   * @param offset 相对于目标时间偏移量(单位：天)，负数表示向前推移，正数表示向后推移
   * @return Date
   */
  public static Date getLastWeekEndDay(Date date, int offset) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);

    cal.add(Calendar.DATE, offset);

    return cal.getTime();
  }

  public static String dateToString(Date date) {
    return dateToString(date, "yyyy-MM-dd");
  }

  public static String dateToString(Date date, String format) {
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.format(date);
  }

  public static Date stringToDate(String date) throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    return sdf.parse(date);
  }

  public static Date stringToDate(String date, String format) throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.parse(date);
  }

}
