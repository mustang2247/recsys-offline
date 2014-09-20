package com.ganqiang.recsys.cluster;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.clustering.UncommonDistributions;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.utils.clustering.ClusterDumper.OUTPUT_FORMAT;

import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.HdfsHelper;

public class Test {

	private static final String HDFS = "hdfs://localhost:9000";

	public static void read() throws IOException {
		Path seqFilePath = new Path(
				"hdfs://localhost:9000//user/hdfs/mix_data/result/clusteredPoints/part-m-00000");
		SequenceFile.Reader reader = new SequenceFile.Reader(
				new Configuration(), Reader.file(seqFilePath));

		IntWritable clusterId = new IntWritable();
		WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();

		while (reader.next(clusterId, value)) {
			System.out.println(value.getVector().get(1) + " belongs to cluster "
					+ clusterId.toString());
		}

		reader.close();
	}


	public static void main(String[] args) throws Exception {
		String localFile = "data/kmeans-sample.txt";
		String inPath = HDFS + "/user/hdfs/mix_data";
		String seqFile = inPath + "/seqfile";
		String seeds = inPath + "/seeds";
		String outPath = inPath + "/result/";
		String clusteredPoints = outPath + "/clusteredPoints";
		new Constants().init();
		new HdfsHelper().init();

		JobConf conf = config();
		HdfsHelper.rmr(inPath);
		HdfsHelper.mkdir(inPath);
		HdfsHelper.put(localFile, inPath);
		HdfsHelper.ls(inPath);

		InputDriver.runJob(new Path(inPath), new Path(seqFile),
				"org.apache.mahout.math.RandomAccessSparseVector");

		int k = 3;
		Path seqFilePath = new Path(seqFile);
		Path clustersSeeds = new Path(seeds);
		DistanceMeasure measure = new EuclideanDistanceMeasure();
		clustersSeeds = RandomSeedGenerator.buildRandom(conf, seqFilePath,
				clustersSeeds, k, measure);
		KMeansDriver.run(conf, seqFilePath, clustersSeeds, new Path(outPath),
				0.01, 10, true, 0.01, false);

		Path outGlobPath = new Path(outPath, "clusters-*-final");
		Path clusteredPointsPath = new Path(clusteredPoints);
		System.out
				.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n",
						outGlobPath, clusteredPointsPath);

		ClusterDumper clusterDumper = new ClusterDumper(outGlobPath,
				clusteredPointsPath);
		clusterDumper.printClusters(null);
		
		read();
	}

	public static JobConf config() {
		JobConf conf = new JobConf(Test.class);
		conf.setJobName("ItemCFHadoop");
		return conf;
	}

}
