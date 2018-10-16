package com.alex.space.common.enums;

/**
 * 日期范围 类型
 *
 * @author Alex Created by Alex on 2018/9/27.
 */
public class DateRange {

  /**
   * 范围类型
   */
  public enum TypeEnum {

    // 距今n时间
    RECENTLY,
    // 当前时间
    CURRENT,
    // 时间区间
    RANGE
  }

  /**
   * 时间单位
   */
  public enum UnitEnum {
    // 天
    DAY,
    // 周
    WEEK,
    // 月
    MONTH
  }


}
