package com.alex.space.spark.livy;

/**
 * Created by Alex on 2017/8/22.
 */
public class LivyConfig {

	/**
	 * Livy Host
	 */
	public static String HOST = "http://127.0.0.1:8998";

	public static String BATCH = LivyConfig.HOST + "/batches";

	public static String SESSION = LivyConfig.HOST + "/sessions";
}
