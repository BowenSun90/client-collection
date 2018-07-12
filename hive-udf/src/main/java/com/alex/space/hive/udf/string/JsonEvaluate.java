package com.alex.space.hive.udf.string;

import com.alex.space.hive.utils.CommonUtil;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple Json value validate
 *
 * @author Alex
 * Created by Alex on 2018/7/12.
 */
public class JsonEvaluate extends UDF {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	public final boolean evaluate(final String arg, final String expression) {
		boolean value = false;
		try {
			Map map = objectMapper.readValue(arg, Map.class);
			value = CommonUtil.evaluate(map, expression);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}

	public static void main(String[] args) {

		// 数据准备
		Map<String, String> record = new HashMap<>(16);
		record.put("k1", "v1");
		record.put("k2", "v21,v22");
		record.put("k3", "34");

		String inputStr = "";
		try {
			inputStr = objectMapper.writeValueAsString(record);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("inputStr -- " + inputStr);

		// 方法验证
		JsonEvaluate func = new JsonEvaluate();
		System.out.println(func.evaluate(inputStr, "${k2.contains(\"v22\")}"));
	}
}
