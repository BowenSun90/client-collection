package com.alex.space.hive.udf.url;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.net.URLDecoder;

/**
 * URL decode
 *
 * @author Alex
 * Created by Alex on 2018/7/12.
 */
public class UrlDecode extends UDF {

	public String evaluate(final String s) {
		if (s == null) {
			return null;
		}
		try {
			return URLDecoder.decode(s, "UTF-8");
		} catch (Exception e) {
			return s;
		}
	}

	public static void main(String args[]) {
		UrlDecode func = new UrlDecode();
		String t = "%E6%B5%8B%E8%AF%95URL%E8%BD%AC%E6%8D%A2%EF%BC%9AHello";

		System.out.println(func.evaluate(t));
	}

}
