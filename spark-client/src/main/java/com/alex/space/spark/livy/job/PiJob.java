package com.alex.space.spark.livy.job;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 2017/8/23.
 */
public class PiJob implements Job<Double>, Function<Integer, Integer>,
		Function2<Integer, Integer, Integer> {

	private final int samples;

	public PiJob(int samples) {
		this.samples = samples;
	}

	@Override
	public Double call(JobContext ctx) {
		List<Integer> sampleList = new ArrayList<>();
		for (int i = 0; i < samples; i++) {
			sampleList.add(i + 1);
		}

		ctx.sc();
		return ctx.sc().parallelize(sampleList).map(this).reduce(this) / (samples * 1.0);
	}

	@Override
	public Integer call(Integer v1, Integer v2) {
		return v1 + v2;
	}

	@Override
	public Integer call(Integer v1) {
		double x = Math.random();
		double y = Math.random();
		return ((x * x + y * y < 1) ? 1 : 0) * v1;
	}
}