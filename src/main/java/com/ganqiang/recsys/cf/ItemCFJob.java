package com.ganqiang.recsys.cf;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.JobEngine;

public class ItemCFJob implements JobEngine{
	
	private static final Logger logger = Logger.getLogger(ItemCFJob.class);

	private String input; 
	private String output;
	private String tmp;
	private boolean flag;

	public ItemCFJob(String input, String output, String tmp, boolean flag){
		this.input = input;
		this.output = output;
		this.tmp = tmp;
		this.flag = flag;
	}

	public void run() throws Exception{
		long startTime = System.currentTimeMillis();
		JobConf conf = new JobConf(ItemCFJob.class);
		conf.setJobName("ItemCF"+System.currentTimeMillis());
		conf.setNumMapTasks(10);
		conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");
		StringBuilder sb = new StringBuilder();
		sb.append("--input ").append(input);
		sb.append(" --output ").append(output);
		if (flag){
			sb.append(" --booleanData true");
		}else{
			sb.append(" --booleanData false");
		}
		sb.append(" --similarityClassname " + Constants.mahout_similarityclassname);
		sb.append(" --tempDir ").append(tmp);
		String[] args = sb.toString().split(" ");
		RecommenderJob job = new RecommenderJob();
		job.setConf(conf);
		job.run(args);
		long endTime = System.currentTimeMillis();
		logger.info("recommdation job ["+conf.getJobName()+"] run finish. it costs"+ (endTime - startTime) / 1000 +"s.");
	}

}
