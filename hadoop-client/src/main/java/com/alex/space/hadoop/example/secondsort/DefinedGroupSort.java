package com.alex.space.hadoop.example.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义排序
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DefinedGroupSort extends WritableComparator {

  protected DefinedGroupSort() {
    super(CombinationKey.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    System.out.println("---------------------进入自定义分组---------------------");
    CombinationKey combinationKey1 = (CombinationKey) a;
    CombinationKey combinationKey2 = (CombinationKey) b;
    System.out.println(
        "---------------------分组结果：" + combinationKey1.getFirstKey()
            .compareTo(combinationKey2.getFirstKey()));

    // 自定义按原始数据中第一个key分组
    return combinationKey1.getFirstKey().compareTo(combinationKey2.getFirstKey());
  }
}
