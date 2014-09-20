package com.ganqiang.recsys.cluster;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.ganqiang.recsys.hbase.HBaseContext;
import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.HdfsHelper;
import com.ganqiang.recsys.util.JobEngine;
import com.ganqiang.recsys.util.ModelNormalizer;

//1.format data : norm and filter
//2.write hdfs
//3.write hbase
public class PrepareClusterJob implements JobEngine{
	
	private static final String split_str = "&";

	public void run() {
		
		try {
			Job job = Job.getInstance(HBaseContext.config, "ClusterPrepareJob");
			job.setJarByClass(PrepareClusterJob.class);

			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			scan.addColumn(Constants.hbase_column_family.getBytes(),
					Constants.hbase_column_yearrate.getBytes());
			scan.addColumn(Constants.hbase_column_family.getBytes(),
					Constants.hbase_column_repaylimittime.getBytes());
			scan.addColumn(Constants.hbase_column_family.getBytes(),
					Constants.hbase_column_progress.getBytes());

			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(Constants.hbase_column_family), 
					Bytes.toBytes(Constants.hbase_column_progress), CompareOp.NOT_EQUAL, Bytes.toBytes("100"));

			scan.setFilter(filter);

			TableMapReduceUtil.initTableMapperJob(Constants.hbase_p2p_table,
					scan, HBaseReadMapper.class, Text.class, Text.class, job);
			TableMapReduceUtil.initTableReducerJob(
					Constants.hbase_cluster_model_table,
					HBaseWriteReducer.class, job);
			job.setNumReduceTasks(1);

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
//		private BytesWritable value = new BytesWritable();
		private Text value = new Text();

		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {
			String yrstr = Bytes.toString(result.getValue(
					Constants.hbase_column_family.getBytes(),
					Constants.hbase_column_yearrate.getBytes()));
			String rltstr = Bytes.toString(result.getValue(
					Constants.hbase_column_family.getBytes(),
					Constants.hbase_column_repaylimittime.getBytes()));
			String yrresult = String.valueOf(ModelNormalizer
					.getYearRateNorm(Double.valueOf(yrstr)));
			String rltresult = String.valueOf(ModelNormalizer
					.getRepayLimitTimeNorm(rltstr));
			
			key.set(row.get());
			value.set(new Text(yrresult+split_str+rltresult));
			
			String inputfile = Constants.hdfs_kmeans_input_file;
			HdfsHelper.writeLine(inputfile, yrresult + " " + rltresult);
			context.write(key, value);
		}
	}

	public static class HBaseWriteReducer extends
			TableReducer<Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text text = values.iterator().next();
			String[] columns = text.toString().split(split_str);
			Put put = new Put(key.getBytes());
			put.add(Bytes.toBytes(Constants.hbase_column_family),
					Bytes.toBytes(Constants.hbase_column_yearrate),
					Bytes.toBytes(columns[0]));
			put.add(Bytes.toBytes(Constants.hbase_column_family),
					Bytes.toBytes(Constants.hbase_column_repaylimittime),
					Bytes.toBytes(columns[1]));
			context.write(NullWritable.get(), put);
		}
	}

}
