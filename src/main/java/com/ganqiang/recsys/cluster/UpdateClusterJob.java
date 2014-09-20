package com.ganqiang.recsys.cluster;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;

import com.ganqiang.recsys.hbase.HBaseContext;
import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.HdfsHelper;
import com.ganqiang.recsys.util.JobEngine;

public final class UpdateClusterJob implements JobEngine{

	public void run() {

		try {
			Job job = Job.getInstance(HBaseContext.config, "UpdateClusterJob");
			job.setJarByClass(UpdateClusterJob.class);

			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			TableMapReduceUtil.initTableMapperJob(
					Constants.hbase_cluster_model_table, scan,
					HBaseReadMapper.class, Text.class, Text.class, job);
			TableMapReduceUtil.initTableReducerJob(
					Constants.hbase_cluster_model_table,
					HBaseWriteReducer.class, job);
			job.setNumReduceTasks(4);

			boolean b = job.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with job!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class HBaseReadMapper extends TableMapper<Text, Text> {

		private Text key = new Text();
		private Text value = new Text();

		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {
			String yrstr = Bytes.toString(result.getValue(
					Constants.hbase_column_family.getBytes(),
					Constants.hbase_column_yearrate.getBytes()));
			String rltstr = Bytes.toString(result.getValue(
					Constants.hbase_column_family.getBytes(),
					Constants.hbase_column_repaylimittime.getBytes()));

			List<String> list = HdfsHelper
					.ls(Constants.hdfs_kmeans_point_output_path);
			String clusterid = null;
			for (String file : list) {
				if (file.contains("_")) {
					continue;
				}
				SequenceFile.Reader reader = new SequenceFile.Reader(
						HBaseContext.config, Reader.file(new Path(file)));
				IntWritable clusterId = new IntWritable();
				WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
				while (reader.next(clusterId, value)) {
					String yearrate = String.valueOf(value.getVector().get(0));
					String repaylimittime = String.valueOf(value.getVector()
							.get(1));
					if (yrstr.equals(yearrate) && rltstr.equals(repaylimittime)) {
						clusterid = clusterId.toString();
						break;
					}
				}

				reader.close();
			}

			key.set(row.get());
			value.set(clusterid);
			clusterid = null;
			context.write(key, value);
		}
	}

	public static class HBaseWriteReducer extends
			TableReducer<Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text text = values.iterator().next();
				Put put = new Put(key.getBytes());
				put.add(Bytes.toBytes(Constants.hbase_column_family),
						Bytes.toBytes(Constants.hbase_column_clusterid),
						text.toString().getBytes());
				context.write(NullWritable.get(), put);

		}
	}
}
