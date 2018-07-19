package com.alex.space.hadoop.example.entity;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * entity job
 *
 * @author Alex Created by Alex on 2018/7/19.
 */
public class EntityReducer extends Reducer<Text, MyEntity, Text, MyEntity> {

	@Override
	public void reduce(Text key, Iterable<MyEntity> values, Context context)
			throws IOException, InterruptedException {
		long upData = 0;
		long downData = 0;
		long upFlow = 0;
		long downFlow = 0;
		for (MyEntity w : values) {
			upData += w.upData;
			downData += w.downData;
			upFlow += w.upFlow;
			downFlow += w.downFlow;
		}
		MyEntity newWs = new MyEntity(String.valueOf(upData), String.valueOf(downData), String.valueOf(upFlow),
				String.valueOf(downFlow));
		context.write(key, newWs);
	}
}
