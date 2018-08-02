package com.alex.space.hive.udf.json;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/**
 * 解析 Json 数组
 *
 * @author Alex
 * Created by Alex on 2018/8/2.
 */
@Description(name = "json_array",
		value = "_FUNC_(array_string) - Convert a string of a JSON-encoded array to a Hive array of strings.")
public class UDFJsonAsArray extends UDF {

	public List<String> evaluate(String jsonString) {
		if (jsonString == null) {
			return null;
		}
		try {
			JSONArray extractObject = new JSONArray(jsonString);
			List<String> result = new ArrayList<>();
			for (int i = 0; i < extractObject.length(); ++i) {
				result.add(extractObject.get(i).toString());
			}
			return result;
		} catch (JSONException e) {
			return null;
		} catch (NumberFormatException e) {
			return null;
		}
	}

	public static void main(String[] args) {
		String str = "[{\"name\":\"a1\",\"code\":\"123\"},{\"name\":\"a2\",\"code\":\"234\"}]";

		List<String> list = new UDFJsonAsArray().evaluate(str);
		list.forEach(System.out::println);

	}

}
