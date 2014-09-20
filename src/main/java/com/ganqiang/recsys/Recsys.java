package com.ganqiang.recsys;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import com.ganqiang.recsys.cf.ItemCFJob;
import com.ganqiang.recsys.cf.UpdateCFJob;
import com.ganqiang.recsys.cluster.KmeansJob;
import com.ganqiang.recsys.cluster.PrepareClusterJob;
import com.ganqiang.recsys.cluster.UpdateClusterJob;
import com.ganqiang.recsys.timer.JobTimer;
import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.HdfsHelper;
import com.ganqiang.recsys.util.Initializer;
import com.ganqiang.recsys.util.StringUtil;
import com.ganqiang.recsys.zk.SwiftWatcher;

public class Recsys {

	private static final Logger logger = Logger.getLogger(Recsys.class);

	static {
		Initializer.setup();
	}

	public static void main(String[] args) {

		// if (args == null) {
		// logger.error("Boot parameters not specified.");
		// System.exit(1);
		// }

		if (StringUtil.isNullOrBlank(Constants.seletor_model)) {
			logger.error("select.model not specified.");
			System.exit(1);
		} else if(Constants.seletor_model.equals("timer")) {//timer mode
			logger.info("Start running timer mode.");
			JobTimer timer = new JobTimer();
			timer.run();
			logger.info("Run timer model finish.");
		} else {//zookeeper mode
			logger.info("Start running zookeeper mode.");
			try {
				SwiftWatcher wa = new SwiftWatcher();
				while (true) {
					ZooKeeper zk = wa.getConnection(Constants.zk_address);
					zk.exists(Constants.zk_start_flag, true);
					TimeUnit.SECONDS.sleep(300);
				}
			} catch (Exception e) {
				logger.info("Start running zookeeper model failed.", e);
			}
			logger.info("Run zookeeper model finish.");
		}

		
	}
	
	public static void runAllTask(){
		try {
			logger.info("Start running itemcf.");
			runItemCF();
			logger.info("run itemcf finish.");
		} catch (Exception e) {
			logger.error("run kmeans method failed.", e);
		}

		try {
			logger.info("begin to running kmeans.");
			runKmeans();
			logger.info("run kmeans finish.");
		} catch (Exception e) {
			logger.error("run kmeans method failed.", e);
		}
	}

	public static void runKmeans() throws Exception {
		String input = Constants.hdfs_kmeans_input;
		String output = Constants.hdfs_kmeans_output;
		String randomssedoutput = Constants.hdfs_kmeans_random_output_path;
		String seqoutput = Constants.hdfs_kmeans_seq_output_path;
		HdfsHelper.rmr(input);
		HdfsHelper.mkdir(input);
		if (HdfsHelper.exists(output)) {
			HdfsHelper.rmr(output);
			logger.warn("the path " + output
					+ " has already exists,it will be delete.");
		}
		new PrepareClusterJob().run();
		new KmeansJob(input, output, randomssedoutput, seqoutput).run();
		new UpdateClusterJob().run();
		
//		HdfsHelper.rmr(output);
//		HdfsHelper.rmr(input);
	}

	public static void runItemCF() throws Exception {
		String input = Constants.hdfs_itemcf_input;
		String output = Constants.hdfs_itemcf_output;
		String tmp = Constants.hdfs_itemcf_tempdir;
		boolean inflag = HdfsHelper.exists(input);
		boolean outflag = HdfsHelper.exists(output);
		boolean tmpflag = HdfsHelper.exists(tmp);
		if (!inflag) {
			logger.error("the path " + input + " is not exists.");
			System.exit(1);
		}
		if (outflag) {
			HdfsHelper.rmr(output);
			logger.warn("the path " + output
					+ " has already exists,it will be delete.");
		}
		if (tmpflag) {
			HdfsHelper.rmr(tmp);
			logger.warn("the path " + tmp
					+ " has already exists,it will be delete.");
		}

		List<String> files = HdfsHelper.ls(input);
		for (String file : files) {
			new ItemCFJob(file, output, tmp, false).run();
			new UpdateCFJob(output).run();

			HdfsHelper.rmr(output);
			HdfsHelper.rmr(tmp);
			// HdfsHelper.rmr(file);
			logger.info("the file [" + file + "] to calculate the complete.");
			break;
		}
	}

	public static void testItemCFClient() throws Exception {
		String input = Constants.hdfs_itemcf_input;
		String output = Constants.hdfs_itemcf_output;
		String tmp = Constants.hdfs_itemcf_tempdir;
		String input_file = Constants.hdfs_itemcf_client_input;

		HdfsHelper.rmr(input);
		HdfsHelper.rmr(output);
		HdfsHelper.mkdir(input);
		HdfsHelper.writeLine(input_file, "1,101,5.0");
		HdfsHelper.writeLine(input_file, "1,102,3.0");
		HdfsHelper.writeLine(input_file, "1,103,2.5");
		HdfsHelper.writeLine(input_file, "2,101,2.0");
		HdfsHelper.writeLine(input_file, "2,102,2.5");
		HdfsHelper.writeLine(input_file, "2,103,5.0");
		HdfsHelper.writeLine(input_file, "2,104,2.0");
		HdfsHelper.writeLine(input_file, "3,101,2.0");
		HdfsHelper.writeLine(input_file, "3,104,4.0");
		HdfsHelper.writeLine(input_file, "3,105,4.5");
		HdfsHelper.writeLine(input_file, "3,107,5.0");
		HdfsHelper.writeLine(input_file, "4,101,5.0");
		HdfsHelper.writeLine(input_file, "4,103,3.0");
		HdfsHelper.writeLine(input_file, "4,104,4.5");
		HdfsHelper.writeLine(input_file, "4,106,4.0");
		HdfsHelper.writeLine(input_file, "5,101,4.0");
		HdfsHelper.writeLine(input_file, "5,102,3.0");
		HdfsHelper.writeLine(input_file, "5,103,2.0");
		HdfsHelper.writeLine(input_file, "5,104,4.0");
		HdfsHelper.writeLine(input_file, "5,105,3.5");
		HdfsHelper.writeLine(input_file, "5,106,4.0");
		HdfsHelper.writeLine(input_file, "6,102,4.0");
		HdfsHelper.writeLine(input_file, "6,103,2.0");
		HdfsHelper.writeLine(input_file, "6,105,3.5");
		HdfsHelper.writeLine(input_file, "6,107,4.0");

		new ItemCFJob(input, output, tmp, false).run();
		new UpdateCFJob(output).run();
	}

	public static void testItemCFBooleanSampleDate() throws Exception {
		String input = Constants.hdfs_itemcf_boolean_input;
		String output = Constants.hdfs_itemcf_boolean_output;
		String tmp = Constants.hdfs_itemcf_tempdir;
		String data = Constants.itemcf_boolean_sample_data;

		HdfsHelper.rmr(input);
		HdfsHelper.rmr(output);
		HdfsHelper.mkdir(input);
		HdfsHelper.put(data, input);

		new ItemCFJob(input, output, tmp, true).run();
		new UpdateCFJob(output).run();
	}

	public static void testItemCFSampleDate() throws Exception {
		String input = Constants.hdfs_itemcf_input;
		String output = Constants.hdfs_itemcf_output;
		String data = Constants.itemcf_sample_data;
		String tmp = Constants.hdfs_itemcf_tempdir;

		HdfsHelper.rmr(input);
		HdfsHelper.rmr(output);
		HdfsHelper.mkdir(input);
		HdfsHelper.put(data, input);

		new ItemCFJob(input, output, tmp, false).run();
		new UpdateCFJob(output).run();
	}

	public static void testKmeansSampleDate() throws Exception {

		String input = Constants.hdfs_kmeans_input;
		String output = Constants.hdfs_kmeans_output;
		String randomssedoutput = Constants.hdfs_kmeans_random_output_path;
		String seqoutput = Constants.hdfs_kmeans_seq_output_path;

		HdfsHelper.rmr(input);
		HdfsHelper.rmr(output);
		HdfsHelper.mkdir(input);
		HdfsHelper.mkdir(randomssedoutput);
		HdfsHelper.put(Constants.kmeans_sample_data, input);

		new PrepareClusterJob().run();
		new KmeansJob(input, output, randomssedoutput, seqoutput).run();
		new UpdateClusterJob().run();
	}

}
