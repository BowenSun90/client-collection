package com.alex.space.hive.utils;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.UnifiedJEXL;

import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Alex
 * Created by Alex on 2018/7/12.
 */
public class CommonUtil {

	/**
	 * 判断数据是否满足条件
	 */
	public static boolean evaluate(Map<String, String> record, String expression) {
		boolean result = (Boolean) evaluateValue(record, expression);
		return result;
	}

	/**
	 * 获取表达式的值
	 */
	public static Object evaluateValue(Map<String, String> record, String expression) {
		JexlEngine jexl = new JexlEngine();
		UnifiedJEXL ujexl = new UnifiedJEXL(jexl);
		UnifiedJEXL.Expression expr = ujexl.parse(expression);
		JexlContext jc = new MapContext();

		for (Entry<String, String> entry : record.entrySet()) {
			jc.set(entry.getKey(), entry.getValue());
		}
		
		return expr.evaluate(jc);
	}
}
