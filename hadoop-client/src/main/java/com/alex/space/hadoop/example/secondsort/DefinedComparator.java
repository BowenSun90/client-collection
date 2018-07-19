package com.alex.space.hadoop.example.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义排序
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class DefinedComparator extends WritableComparator {
	protected DefinedComparator() {
		super(CombinationKey.class, true);
	}

	/**
	 * 第一列按升序排列，第二列也按升序排列
	 */
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable a, WritableComparable b) {
		System.out.println("------------------进入二次排序-------------------");
		CombinationKey c1 = (CombinationKey) a;
		CombinationKey c2 = (CombinationKey) b;
		int minus = c1.getFirstKey().compareTo(c2.getFirstKey());

		if (minus != 0) {
			return minus;
		} else {
			return c1.getSecondKey().get() - c2.getSecondKey().get();
		}
	}
}
