package com.alex.space.spark.livy;

import com.alex.space.spark.livy.job.PiJob;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientBuilder;

import java.io.File;
import java.net.URI;

/**
 * LivyClient
 * <p>
 * Created by Alex on 2017/8/23.
 */
public class LivyClientDemo {

	public static void main(String[] args) throws Exception {
		String livyUrl = LivyConfig.HOST;

		LivyClient client = new LivyClientBuilder()
				.setURI(new URI(livyUrl))
				.build();

		try {

			String piJar = "/SparkPi.jar";
			System.err.printf("Uploading %s to the Spark context...\n", piJar);
			client.uploadJar(new File(piJar)).get();

			int samples = 10;
			System.err.printf("Running PiJob with %d samples...\n", samples);
			double pi = client.submit(new PiJob(samples)).get();

			System.out.println("Pi is roughly: " + pi);
		} finally {
			client.stop(true);
		}
	}
}
