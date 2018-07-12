package com.alex.space.hive.udf.url;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.net.URLEncoder;

/**
 * URL encode
 *
 * @author Alex
 * Created by Alex on 2018/7/12.
 */
public class UrlEncode extends UDF {

	public String evaluate(final String s) {
		if (s == null) {
			return null;
		}
		try {
			return URLEncoder.encode(s, "UTF-8");
		} catch (Exception e) {
			return s;
		}
	}

	public static void main(String args[]) {
		UrlEncode func = new UrlEncode();

		System.out.println(func.evaluate("测试URL转换：Hello"));
	}

}
