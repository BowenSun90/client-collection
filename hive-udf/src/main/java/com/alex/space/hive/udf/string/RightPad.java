package com.alex.space.hive.udf.string;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * String Right pad
 *
 * @author Alex
 * Created by Alex on 2018/7/12.
 */
public class RightPad extends UDF {

	public final String evaluate(final String arg, final Integer size, final String placeHolder) {
		return StringUtils.rightPad(arg, size, placeHolder);
	}

	public static void main(String[] args) {
		RightPad func = new RightPad();
		System.out.println(func.evaluate("1", 12, "0"));
	}

}
