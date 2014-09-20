package com.ganqiang.recsys.util;

public final class ModelNormalizer {

	public static Double getYearRateNorm(Double d) {
		Double result = 0d;
		if (d >= 0 && d <= 5) {
			result = 0.1;
		} else if (d > 5 && d <= 6) {
			result = 0.15;
		} else if (d > 6 && d <= 7) {
			result = 0.2;
		} else if (d > 7 && d <= 8) {
			result = 0.25;
		} else if (d > 8 && d <= 9) {
			result = 0.3;
	 } else if (d > 9 && d <= 10) {
			result = 0.35;
		} else if (d > 10 && d <= 11) {
			result = 0.4;
		} else if (d > 11 && d <= 12) {
			result = 0.45;
		} else if (d > 12 && d <= 13) {
			result = 0.5;
		} else if (d > 13 && d <= 14) {
			result = 0.55;
		} else if (d > 14 && d <= 15) {
			result = 0.6;
		} else if (d > 15 && d <= 16) {
			result = 0.65;
		} else if (d > 16 && d <= 17) {
			result = 0.7;
		} else if (d > 17 && d <= 18) {
			result = 0.75;
		} else if (d > 18 && d <= 19) {
			result = 0.8;
		} else if (d > 19 && d <= 20) {
			result = 0.85;
		} else if (d > 20 && d <= 25) {
			result = 0.9;
		} else if (d > 25 && d <= 30) {
			result = 0.95;
		} else{
			result = 1.0;
		}
		return result;
	}

	public static Double getRepayLimitTimeNorm(String date) {
		Double result = 0d;
		Double time = DateUtil.getNormTime(date);
		if(time > 0 && time <= 0.1){//10天内
			result = 0.1;
		}else if(time > 0.1 && time <= 0.5){//0.5个月内
			result = 0.15;
		}else if(time > 0.5 && time <= 1){//1个月内
			result = 0.2;
		}else if(time > 1 && time <= 1.5){//1.5个月内
			result = 0.25;
		}else if(time > 1.5 && time <= 2){//2个月内
			result = 0.3;
		}else if(time > 2 && time <= 3){//3个月内
			result = 0.35;
		}else if(time > 3 && time <= 4){//4个月内
			result = 0.4;
		}else if(time > 4 && time <= 4.5){//4.5个月内
			result = 0.45;
		}else if(time > 4.5 && time <= 5){//5个月内
			result = 0.5;
		}else if(time > 5 && time <= 6){//6个月内
			result = 0.55;
		}else if(time > 6 && time <= 7){//7个月内
			result = 0.6;
		}else if(time > 7 && time <= 8){//8个月内
			result = 0.65;
		}else if(time > 8 && time <= 9){//9个月内
			result = 0.7;
		}else if(time > 9 && time <= 10){//10个月内
			result = 0.75;
		}else if(time > 10 && time <= 11){//11个月内
			result = 0.8;
		}else if(time > 11 && time <= 12){//12个月内
			result = 0.85;
		}else if(time > 12 && time <= 18){//18个月内
			result = 0.9;
		}else if(time > 18 && time <= 24){//24个月内
			result = 0.95;
		}else {
			result = 1.0;
		}
		return result;
	}

}
