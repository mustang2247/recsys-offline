package com.ganqiang.recsys.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class DateUtil {
	
	public static final String yyyyMMddHHmmss = "yyyy-MM-dd HH:mm:ss";

	public static String getCurrentTime() {
		Date date = new Date();
		SimpleDateFormat format = new SimpleDateFormat(yyyyMMddHHmmss);
		return format.format(date);
	}
	
	public static Date parse(String date) {
		SimpleDateFormat format = new SimpleDateFormat(yyyyMMddHHmmss);
		try {
			return format.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Double getNormTime(String date) {
		int day = 0;
		int month = 0;
		String daystr = "";
		if (!date.contains("月") && date.contains("天")) {
			day = Integer.valueOf(date.replaceAll("天", "").replaceAll(" ", ""));
			if (day >= 30) {
				Integer jinwei = ((Double)CalculateUtil.div(day, 30)).intValue();
				month = jinwei + month;
				day = day - jinwei * 30 ;
			}
			if (String.valueOf(day).length() == 1) {
				daystr = "0" + day;
			}else{
				daystr = "" + day;
			}
		} else if (date.contains("月") && date.contains("天")) {
			String[] splits = null;
			if(date.contains("月")){
				splits = date.split("月");
			} else if(date.contains("个月")){
				splits = date.split("个月");
			}
			month = Integer.valueOf(splits[0]);
			day = Integer.valueOf(splits[0].replaceAll("天", "").replaceAll(" ", ""));
			if (String.valueOf(day).length() == 1) {
				daystr = "0" + day;
			}else{
				daystr = "" + day;
			}
		} else if (date.contains("月") && !date.contains("天")){
			month = Integer.valueOf(date.replaceAll("月", "").replaceAll("个", "").replaceAll(" ", ""));
		}
		String str = month + "." + daystr;
		return Double.valueOf(str);
	}
	
}
