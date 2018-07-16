package com.alex.space.spark.livy;

import com.alex.space.spark.livy.utils.LivyUtils;

import java.util.List;

/**
 * Livy http request app
 * <p>
 * Created by Alex on 2017/8/22.
 */
public class LivyUtilsDemo {

	public static void main(String[] args) {

		try {

			List<Integer> ids = LivyUtils.getBatches();
			for (Integer id : ids) {
				LivyUtils.getBatch(id);

				LivyUtils.getBatchState(id);

				LivyUtils.getBatchLog(id);

				LivyUtils.deleteBatch(id);
			}

			LivyUtils.postBatches("wordCount.jar", "WordCount");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
