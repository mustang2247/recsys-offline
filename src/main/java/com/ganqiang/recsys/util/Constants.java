package com.ganqiang.recsys.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.mahout.common.distance.ChebyshevDistanceMeasure;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.MahalanobisDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.common.distance.MinkowskiDistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.common.distance.WeightedEuclideanDistanceMeasure;
import org.apache.mahout.common.distance.WeightedManhattanDistanceMeasure;

public final class Constants implements Initializable{

	private static final Logger logger = Logger.getLogger(Constants.class);

	private static Properties p = new Properties();

	public static String seletor_model = null;

	public static Date timer_start_time= null;
	public static Long timer_interval= null;
	
	public static String zk_address = null;
	public static Integer zk_retry = null;
	public static final String zk_start_flag = "/queue/start";
	public static final String zk_znode = "/queue";

	public static String hdfs_address = null;

	public static String mahout_similarityclassname = null;
	public static DistanceMeasure mahout_distanceclass = null;

	public static String hbase_master = null;
	public static String hbase_zk_quorum = null;
	public static Integer hbase_zk_client_port = null;

	public static final String hbase_user_item_pref_table = "user_item_pref";
	public static final String hbase_user_item_table = "user_item";
	public static final String hbase_cluster_model_table = "cluster_model";
	public static final String hbase_p2p_table = "p2p";
	public static final String[] hbase_tables = { "user_item", "user_item_pref" };
	public static final String hbase_column_family = "cf";
	public static final String hbase_column_yearrate = "YEAR_RATE";
	public static final String hbase_column_clusterid = "CLUSTER_ID";
	public static final String hbase_column_repaylimittime = "REPAY_LIMIT_TIME";
	public static final String hbase_column_progress = "PROGRESS";
	
	public static final String similarity_class_path = "org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.";

	public static String hdfs_itemcf_input = null;
	public static String hdfs_itemcf_output = null;
	public static String hdfs_itemcf_boolean_input = null;
	public static String hdfs_itemcf_boolean_output = null;
	public static String hdfs_itemcf_tempdir = null;
	public static String hdfs_itemcf_client_input = null;

	public static String hdfs_kmeans_input = null;
	public static String hdfs_kmeans_output = null;
	public static String hdfs_kmeans_random_output_path = null;
	public static String hdfs_kmeans_seq_output_path = null;
	public static String hdfs_kmeans_point_output_path = null;
	public static String hdfs_kmeans_input_file = null;
	
	public static Integer kmeans_k = null;
	public static Double kmeans_convergence_delta = null; //收敛值，每次迭代质心间的距离
	public static Integer kmeans_max_iterations = null; //最大迭代次数
	public static final Double kmeans_clusterClassificationThreshold = 0.1d; //严格系数：0---1
	public static final Boolean kmeans_run_clustering = true; //是否执行聚类
	public static final Boolean kmeans_run_sequential = false; //同步或异步计算

	public static final String itemcf_boolean_sample_data = System.getProperty("user.dir") + "/data/itemcf-boolean-sample.txt";
	public static final String itemcf_sample_data = System.getProperty("user.dir") + "/data/itemcf-sample.txt";
	public static final String kmeans_sample_data = System.getProperty("user.dir") + "/data/kmeans-sample.txt";

	public void init() {
		init(System.getProperty("user.dir") + "/conf/log4j.conf");
		init(System.getProperty("user.dir") + "/conf/system.conf");
		PropertyConfigurator.configure(p);
		
		seletor_model = getProperty("select.model","timer");
		
		timer_start_time = DateUtil.parse(getProperty("timer.start_time", DateUtil.getCurrentTime()));
		timer_interval = Long.valueOf(CalculateUtil.parseExp(getProperty("timer.interval", "0")));

		zk_address = getProperty("zookeeper.address","hdfs://localhost:9000");
		zk_retry = Integer.valueOf(getProperty("zookeeper.retry","3"));
		
		hdfs_address = getProperty("hdfs.address","hdfs://localhost:9000");
		hdfs_itemcf_input = getProperty("hdfs.itemcf.input","hdfs://localhost:9000/itemcf/input");
		hdfs_itemcf_client_input = hdfs_itemcf_input + "/data";
		hdfs_itemcf_output = getProperty("hdfs.itemcf.output","hdfs://localhost:9000/itemcf/output");
		hdfs_itemcf_boolean_input = Constants.hdfs_address + "/boolean_input";
		hdfs_itemcf_boolean_output = Constants.hdfs_address + "/boolean_output";
		hdfs_itemcf_tempdir = getProperty("hdfs.itemcf.tmp","hdfs://localhost:9000/tmp");

		kmeans_k = Integer.valueOf(getProperty("kmeans.k","3"));
		kmeans_convergence_delta = Double.valueOf(getProperty("kmeans.con_delta","0.5"));
		kmeans_max_iterations = Integer.valueOf(getProperty("kmeans.max_itera","3"));

		hdfs_kmeans_input = getProperty("hdfs.kmeans.input","hdfs://localhost:9000/kmeans/input");
		hdfs_kmeans_output = getProperty("hdfs.kmeans.output","hdfs://localhost:9000/kmeans/output");
		hdfs_kmeans_random_output_path = hdfs_kmeans_output + "/cluster";
		hdfs_kmeans_seq_output_path = hdfs_kmeans_output + "/randomseed";
		hdfs_kmeans_point_output_path = hdfs_kmeans_output + "/clusteredPoints";
		hdfs_kmeans_input_file = hdfs_kmeans_input + "/data";
		
		hbase_master = getProperty("hbase.master","localhost");
		hbase_zk_quorum = getProperty("hbase.zk_quorum","localhost");
		hbase_zk_client_port = Integer.valueOf(getProperty("hbase.zk_client_port", "2181"));
		
		mahout_similarityclassname = similarity_class_path + getSimilarityClassName(getProperty("mahout.itemcf.similarity","EuclideanDistanceSimilarity"));
		mahout_distanceclass = getDistanceClass(getProperty("mahout.kmeans.distance","manhattan"));
		
		logger.info("configuration files init finish.");
	}

	private DistanceMeasure getDistanceClass(String shortName){
		if (shortName.equalsIgnoreCase("manhattan")){
			return new ManhattanDistanceMeasure();
		} else if(shortName.equalsIgnoreCase("cosine")){
			return new CosineDistanceMeasure();
		} else if(shortName.equalsIgnoreCase("chebyshev")){
			return new ChebyshevDistanceMeasure();
		} else if(shortName.equalsIgnoreCase("euclidean")){
			return new EuclideanDistanceMeasure();
		} else if(shortName.equalsIgnoreCase("mahalanobis")){
			return new MahalanobisDistanceMeasure();
		} else if(shortName.equalsIgnoreCase("minkowski")){
			return new MinkowskiDistanceMeasure();
		} else if(shortName.equalsIgnoreCase("squaredeuclidean")){
			return new SquaredEuclideanDistanceMeasure();
		} else if(shortName.equalsIgnoreCase("tanimoto")){
			return new TanimotoDistanceMeasure();
		} else if(shortName.equalsIgnoreCase("weightedeuclidean")){
			return new WeightedEuclideanDistanceMeasure();
		} else if(shortName.equalsIgnoreCase("weightedmanhattan")){
			return new WeightedManhattanDistanceMeasure();
		} 
		return new ManhattanDistanceMeasure();
	}

	private String getSimilarityClassName(String shortName){
		if (shortName.equalsIgnoreCase("euclidean")){
			return "EuclideanDistanceSimilarity";
		} else if(shortName.equalsIgnoreCase("pearson")){
			return "PearsonCorrelationSimilarity";
		} else if(shortName.equalsIgnoreCase("loglike")){
			return "LoglikelihoodSimilarity";
		} else if(shortName.equalsIgnoreCase("cosine")){
			return "CosineSimilarity";
		} else if(shortName.equalsIgnoreCase("cooccurrence")){
			return "CooccurrenceCountSimilarity";
		} else if(shortName.equalsIgnoreCase("cityblock")){
			return "CityBlockSimilarity";
		} else if(shortName.equalsIgnoreCase("tanimoto")){
			return "TanimotoCoefficientSimilarity";
		}
		return "EuclideanDistanceSimilarity";
	}

	private void init(String propertyFileName) {
		InputStream in = null;
		try {
			in = new FileInputStream(propertyFileName);
			if (in != null){
				p.load(in);
			}
		} catch (IOException e) {
			logger.error("load " + propertyFileName + " into Contants error");
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private String getProperty(String key, String defaultValue) {
		return p.getProperty(key, defaultValue);
	}

}
