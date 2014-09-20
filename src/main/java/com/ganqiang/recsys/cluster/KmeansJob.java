package com.ganqiang.recsys.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;

import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.JobEngine;

public class KmeansJob implements JobEngine {

	private String input; 
	private String output; 
	private String randomseedoutput; 
	private String seqoutput;
	
	public KmeansJob(String input, String output, String randomseedoutput, String seqoutput){
		this.input = input;
		this.output = output;
		this.randomseedoutput = randomseedoutput;
		this.seqoutput = seqoutput;
	}

	@Override
	public void run() throws Exception {

		Path inputpath=new Path(input);
		Path outputpath=new Path(output);
		Path randomseedoutpath=new Path(randomseedoutput);
		Path seqoutpath = new Path(seqoutput);

		InputDriver.runJob(inputpath, seqoutpath, "org.apache.mahout.math.RandomAccessSparseVector");
		Configuration conf=new Configuration();

		Path clustersSeeds = RandomSeedGenerator.buildRandom(conf, seqoutpath,	randomseedoutpath, Constants.kmeans_k, Constants.mahout_distanceclass);
		Double convergenceDelta = Constants.kmeans_convergence_delta;
		int maxIterations = Constants.kmeans_max_iterations;
		boolean runClustering = Constants.kmeans_run_clustering;
		double clusterClassificationThreshold = Constants.kmeans_clusterClassificationThreshold;
		boolean runSequential = Constants.kmeans_run_sequential;
		
		KMeansDriver.run(conf, seqoutpath, clustersSeeds, outputpath, convergenceDelta, maxIterations, runClustering, clusterClassificationThreshold, runSequential);
	}

}
