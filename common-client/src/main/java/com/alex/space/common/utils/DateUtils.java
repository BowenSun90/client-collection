package com.alex.space.common.utils;

import com.alex.space.common.enums.DateRange.UnitEnum;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;

/**
 * @author Alex Created by Alex on 2018/9/13.
 */
public class DateUtils {

  /**
   * 默认日期格式
   */
  static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

  /**
   * 默认日期+时间格式
   */
  static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd hh:mm:ss";

  public static String dateToString(Date date) {
    return dateToString(date, DEFAULT_DATE_FORMAT);
  }

  public static String dateToString(Date date, String format) {
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.format(date);
  }

  public static Date stringToDate(String date) throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
    return sdf.parse(date);
  }

  public static Date stringToDate(String date, String format) throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.parse(date);
  }

  public static boolean inNextWeek(Date date) {
    Date now = new Date();
    Date start = getNextWeekStartDay(now);
    Date end = getNextWeekEndDay(now);

    return date.after(start) && date.before(end);
  }

  public static boolean inNextMonth(Date date) {
    Date now = new Date();
    Date start = getNextMonthStartDay(now);
    Date end = getNextMonthEndDay(now);

    return date.after(start) && date.before(end);
  }

  /**
   * 获取下一周第一天
   */
  public static Date getNextWeekStartDay(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.setFirstDayOfWeek(Calendar.MONDAY);
    cal.set(Calendar.HOUR, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    cal.add(Calendar.WEEK_OF_YEAR, 1);

    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

    return cal.getTime();
  }

  /**
   * 获取下一周最后一天
   */
  public static Date getNextWeekEndDay(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.setFirstDayOfWeek(Calendar.MONDAY);
    cal.set(Calendar.HOUR, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    cal.add(Calendar.WEEK_OF_YEAR, 1);
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);

    return cal.getTime();
  }

  /**
   * 获取下一月第一天
   */
  public static Date getNextMonthStartDay(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH);
    cal.clear();

    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month);

    cal.roll(Calendar.DAY_OF_MONTH, -1);
    cal.add(Calendar.DATE, 1);

    return cal.getTime();
  }

  /**
   * 获取下一月最后一天
   */
  public static Date getNextMonthEndDay(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    cal.add(Calendar.MONTH, 1);
    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH);
    cal.clear();

    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month);
    cal.roll(Calendar.DAY_OF_MONTH, -1);

    return cal.getTime();
  }

  /**
   * 获取当前周第一天
   */
  public static Date getCurWeekStartDay(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.setFirstDayOfWeek(Calendar.MONDAY);
    cal.set(Calendar.HOUR, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

    return cal.getTime();
  }

  /**
   * 获取当前周最后一天
   */
  public static Date getCurWeekEndDay(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.setFirstDayOfWeek(Calendar.MONDAY);
    cal.set(Calendar.HOUR, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);

    return cal.getTime();
  }

  /**
   * 获取当前周第一天
   */
  public static Date getCurMonthStartDay(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH);
    cal.clear();

    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month);

    cal.roll(Calendar.DAY_OF_MONTH, 0);

    return cal.getTime();
  }

  /**
   * 获取下一月最后一天
   */
  public static Date getCurMonthEndDay(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH);
    cal.clear();

    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month);
    cal.roll(Calendar.DAY_OF_MONTH, -1);
    cal.add(Calendar.DAY_OF_YEAR, -1);

    return cal.getTime();
  }

  /**
   * 获取时间间隔天数
   */
  public static int getDateInterval(Date date1, Date date2) {
    LocalDate d1 = LocalDateTime.ofInstant(date1.toInstant(), ZoneId.systemDefault()).toLocalDate();
    LocalDate d2 = LocalDateTime.ofInstant(date2.toInstant(), ZoneId.systemDefault()).toLocalDate();
    if (date1.before(date2)) {
      Period p = Period.between(d1, d2);
      return p.getDays();
    } else {
      Period p = Period.between(d2, d1);
      return p.getDays();
    }
  }

  /**
   * 获取距今N时间的日期
   *
   * @param date 结束时间
   * @param unitEnum 时间类型
   * @param interval 时间间隔
   */
  public static Date getRecentlyDate(Date date, UnitEnum unitEnum, int interval) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    if (unitEnum == UnitEnum.DAY) {
      cal.add(Calendar.DAY_OF_YEAR, interval * -1);
    } else if (unitEnum == UnitEnum.MONTH) {
      cal.add(Calendar.MONTH, interval * -1);
    } else if (unitEnum == UnitEnum.WEEK) {
      cal.add(Calendar.DAY_OF_YEAR, interval * 7 * -1);
    }

    return cal.getTime();

  }

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

}
