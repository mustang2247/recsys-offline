package com.ganqiang.recsys.timer;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import com.ganqiang.recsys.Recsys;
import com.ganqiang.recsys.util.Constants;

public class JobTimer {

	public void run() {
		Date starttime = Constants.timer_start_time;
		Long interval = Constants.timer_interval;
		
		Timer timer = new Timer();
		Task task = new Task();
		if (interval == null && starttime == null) {
			timer.schedule(task, 0l);
		} else if (interval == null || interval == 0) {
			timer.schedule(task, starttime); System.out.println("ww");
		} else {
			timer.schedule(task, starttime, interval * 1000);
		}
	}

	public static class Task extends TimerTask {

		@Override
		public void run() {
			Recsys.runAllTask();
		}

	}


}
