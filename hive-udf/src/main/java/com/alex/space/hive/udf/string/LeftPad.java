package com.alex.space.hive.udf.string;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * String Left pad
 *
 * @author Alex
 * Created by Alex on 2018/7/12.
 */
public class LeftPad extends UDF {

	public final String evaluate(final String arg, final Integer size, final String placeHolder) {
		return StringUtils.leftPad(arg, size, placeHolder);
	}

	public static void main(String[] args) {
		LeftPad func = new LeftPad();
		System.out.println(func.evaluate("1", 12, "0"));
	}

}
