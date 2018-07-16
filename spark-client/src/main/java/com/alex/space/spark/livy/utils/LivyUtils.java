package com.alex.space.spark.livy.utils;

import com.alex.space.spark.livy.LivyConfig;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 2017/8/22.
 */
public class LivyUtils {

	/**
	 * GET /batches
	 */
	public static List<Integer> getBatches() throws IOException, JSONException {
		List<Integer> ids = new ArrayList<>();

		String requestURL = LivyConfig.BATCH;
		System.out.println(requestURL);

		Content content = Request.Get(requestURL).execute().returnContent();
		JSONObject jsonObj = new JSONObject(content.asString());
		JSONArray sessions = jsonObj.getJSONArray("sessions");

		String format = "Batch Id: %s, State: %s";
		for (int i = 0; i < sessions.length(); i++) {
			JSONObject session = sessions.getJSONObject(i);
			Integer id = session.getInt("id");
			String state = session.getString("state");
			System.out.println(String.format(format, id, state));
			ids.add(id);
		}

		return ids;
	}

	/**
	 * GET /batches/{batchId}
	 */
	public static void getBatch(Integer id) throws IOException {
		String requestURL = LivyConfig.BATCH + "/" + id;
		System.out.println(requestURL);

		Content content = Request.Get(requestURL).execute().returnContent();

		System.out.println(content.asString());
	}

	/**
	 * GET /batches/{batchId}/log
	 */
	public static void getBatchLog(Integer id) throws IOException {
		String requestURL = LivyConfig.BATCH + "/" + id + "/log";
		System.out.println(requestURL);

		Content content = Request.Get(requestURL).execute().returnContent();

		System.out.println(content.asString());
	}

	/**
	 * GET /batches/{batchId}/state
	 */
	public static void getBatchState(Integer id) throws IOException {
		String requestURL = LivyConfig.BATCH + "/" + id + "/state";
		System.out.println(requestURL);

		Content content = Request.Get(requestURL).execute().returnContent();

		System.out.println(content.asString());
	}

	/**
	 * DELETE /batches/{batchId}
	 */
	public static void deleteBatch(Integer id) throws IOException {
		String requestURL = LivyConfig.BATCH + "/" + id;
		System.out.println(requestURL);

		Content content = Request.Delete(requestURL).execute().returnContent();
		System.out.println(content.asString());
	}

	/**
	 * POST /batches
	 */
	public static void postBatches(String jarHdfsPath, String mainClass) throws IOException, JSONException {
		String requestURL = LivyConfig.BATCH;

		JSONObject data = new JSONObject();
		data.put("file", jarHdfsPath);
		data.put("className", mainClass);
		data.put("name", "WordCount");
		data.put("executorMemory", "1G");
		data.put("driverMemory", "1G");
		data.put("numExecutors", 1);
		data.put("executorCores", 1);

		post(requestURL, data.toString());
	}

	public static String post(String urlPath, String json) throws IOException {
		try {
			URL url = new URL(urlPath);
			HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setDoOutput(true);
			urlConnection.setRequestProperty("content-type", "application/json");

			OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
			out.write(json);
			out.flush();
			out.close();

			InputStream inputStream = urlConnection.getInputStream();
			String encoding = urlConnection.getContentEncoding();
			String body = IOUtils.toString(inputStream, encoding);

			return body;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}


}
