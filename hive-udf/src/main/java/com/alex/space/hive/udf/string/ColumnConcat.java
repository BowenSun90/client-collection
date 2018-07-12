package com.alex.space.hive.udf.string;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * String concat
 *
 * @author Alex
 * Created by Alex on 2018/7/12.
 */
public class ColumnConcat extends UDF {

	public final String evaluate(final String separator, final Object... columns) {
		return StringUtils.join(columns, separator);
	}

	public static void main(String[] args) {
		ColumnConcat func = new ColumnConcat();
		System.out.println(func.evaluate("#", 1, 2, 3, "4"));
	}
}
