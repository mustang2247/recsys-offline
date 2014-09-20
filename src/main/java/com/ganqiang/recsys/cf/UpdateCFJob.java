package com.ganqiang.recsys.cf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.JobEngine;
import com.ganqiang.recsys.util.StringUtil;

public class UpdateCFJob implements JobEngine{
	
	private static final Logger logger= Logger.getLogger(UpdateCFJob.class);
	
	private String input;

	public UpdateCFJob(String input){
		this.input = input;
	}
	
	public void run() throws Exception{
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		conf.set(TableOutputFormat.OUTPUT_TABLE, Constants.hbase_user_item_pref_table);
		Job job = Job.getInstance(conf, "hbasewriter"+System.currentTimeMillis());
		job.setJarByClass(UpdateCFJob.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(HBaseWriteReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);  
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		long endTime = System.currentTimeMillis();
		boolean isFinish = job.waitForCompletion(true);
		if(isFinish){
			logger.info("UpdateCFJob job ["+job.getJobName()+"] run finish.it costs"+ (endTime - startTime) / 1000 +"s.");
		} else {
			logger.error("UpdateCFJob job ["+job.getJobName()+"] run failed.");
		}
	}

	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {

		private IntWritable userid = new IntWritable();
		private Text itempref = new Text();

		public void map(Object key, Text value, Context context)
		        throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString()
			        .replaceAll("\\[", "").replaceAll("\\]", ""));
			while (itr.hasMoreTokens()) {
				userid.set(Integer.valueOf(itr.nextToken()));	
				itempref.set(itr.nextToken());
				context.write(userid, itempref);
			}
		}
	}

	public static class HBaseWriteReducer extends TableReducer<IntWritable, Text, NullWritable> {
		
		public void reduce(IntWritable key, Iterable<Text> values,
		        Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				String[] array = val.toString().split(",");
				String rowKey = StringUtil.MD5(key.toString() + ":" + val.toString());
				Put put = new Put(Bytes.toBytes(rowKey)); 
				put.add(Bytes.toBytes(Constants.hbase_column_family),Bytes.toBytes("userid"),Bytes.toBytes(String.valueOf(key)));  
				for (String pair : array) {
					String[] itemprefs = pair.split("\\:");
					put.add(Bytes.toBytes(Constants.hbase_column_family),Bytes.toBytes("itemid"),Bytes.toBytes(String.valueOf(itemprefs[0])));
					put.add(Bytes.toBytes(Constants.hbase_column_family),Bytes.toBytes("pref"),Bytes.toBytes(String.valueOf(itemprefs[1]))); 
				}
				context.write(NullWritable.get(), put);  
			}
		}
	}

	public static void main(String[] args) throws Exception {
//		Configuration conf = new Configuration();
//		conf.set(TableOutputFormat.OUTPUT_TABLE, Constants.hbase_user_item_pref_table);
//		Job job = Job.getInstance(conf);
//		job.setJarByClass(HBaseWriterJob.class);
//		job.setMapperClass(TokenizerMapper.class);
//		job.setReducerClass(HBaseWriteReducer.class);
//		job.setMapOutputKeyClass(IntWritable.class);  
//		job.setMapOutputValueClass(Text.class);
//		job.setOutputFormatClass(TableOutputFormat.class);
//		FileInputFormat.addInputPath(job, new Path(result));
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
