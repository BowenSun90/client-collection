package com.alex.space.hive.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Calendar;
import java.util.Date;

/**
 * Long value to week string YYYY-WW
 *
 * @author Alex
 * Created by Alex on 2018/7/12.
 */
public class Long2Week extends UDF {

	public final String evaluate(final Long time) {
		if (time == null || time < 0) {
			return null;
		}

		try {
			Date date = new Date(time);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			int week = cal.get(Calendar.WEEK_OF_YEAR);
			int year = cal.get(Calendar.YEAR);
			return year + "-" + week;

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String[] args) {
		Long2Week func = new Long2Week();
		System.out.println(func.evaluate(Long.valueOf("1319702640126")));
	}
}
