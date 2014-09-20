package com.ganqiang.recsys.util;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

public final class HdfsHelper implements Initializable {

	private static final Logger log = Logger.getLogger(HdfsHelper.class);

	private volatile static FileSystem hdfs = null;

	@Override
	public void init() {
		Configuration config = new Configuration();
		config.setBoolean("dfs.support.append", true);
		config.set("dfs.client.block.write.replace-datanode-on-failure.policy",
				"NEVER");
		config.setBoolean(
				"dfs.client.block.write.replace-datanode-on-failure.enable",
				true);
		try {
			hdfs = FileSystem.newInstance(URI.create(Constants.hdfs_address),
					config);
		} catch (Exception e) {
			log.error("loading hdfs conf error.", e);
		}
		log.info("hdfs init finish.");
	}

	public static void main(String[] args) throws Exception {
		HdfsHelper hd = new HdfsHelper();
		hd.init();
		writeLine("hdfs://localhost:9000/kmeans/input/abc", "hello");
		// hdfs.close();
		System.out.println("....");
	}

	public static void lsNodes(FileSystem fs) {
		DistributedFileSystem dfs = (DistributedFileSystem) fs;
		try {
			DatanodeInfo[] infos = dfs.getDataNodeStats();
			for (DatanodeInfo node : infos) {
				log.info("HostName: " + node.getHostName() + "  "
						+ node.getDatanodeReport());
			}
		} catch (Exception e) {
			log.error("list hdfs nodes failed.", e);
		}
	}

	public static void lsConfig(FileSystem fs) {
		Iterator<Entry<String, String>> entrys = fs.getConf().iterator();
		while (entrys.hasNext()) {
			Entry<String, String> item = entrys.next();
			log.info(item.getKey() + ": " + item.getValue());
		}
	}

	public static void mkdir(String dir) {
		Path path = new Path(dir);
		// FsPermission fp = FsPermission.getDefault();
		boolean flag = false;
		try {
			flag = hdfs.mkdirs(path);
			if (flag) {
				log.info("create hdfs directory " + dir + " successed.");
			} else {
				log.info("create hdfs directory " + dir + " failed.");
			}
		} catch (Exception e) {
			log.error("create hdfs directory " + dir + " failed.", e);
		}
	}

	public static void rmr(String dir) {
		Path src = new Path(dir);
		boolean flag = false;
		try {
			flag = hdfs.delete(src, true);
			if (flag) {
				log.info("remove hdfs directory " + dir + " successed.");
			} else {
				log.info("remove hdfs directory " + dir + " failed.");
			}
		} catch (Exception e) {
			log.error("remove hdfs directory " + dir + " failed.", e);
		}
	}

	public static void put(String local, String remote) {
		Path dst = new Path(remote);
		Path src = new Path(local);
		try {
			hdfs.copyFromLocalFile(false, true, src, dst);
			log.info("put hdfs from  " + local + " to  " + remote
					+ " successed. ");
		} catch (Exception e) {
			log.error("put hdfs from " + local + " to  " + remote + " failed.",
					e);
		}
	}

	public static void getMerge(String local, String remote) {
		Path dst = new Path(remote);
		Path src = new Path(local);
		try {
			hdfs.copyToLocalFile(false, dst, src);
			log.info("getMerge from " + remote + " to  " + local
					+ " successed. ");
		} catch (Exception e) {
			log.error("getMerge from " + remote + " to  " + local + " failed :");
		}
	}

	public static List<String> ls(String path) {
		List<String> list = new ArrayList<String>();
		Path dst = new Path(path);
		try {
			FileStatus[] fList = hdfs.listStatus(dst);
			for (FileStatus f : fList) {
				list.add(f.getPath().toString());
			}
		} catch (Exception e) {
			log.error("ls hdfs failed :", e);
		}
		return list;
	}

	public static void writeLine(String path, String data) {
		FSDataOutputStream dos = null;
		try {
			Path dst = new Path(path);
			hdfs.createNewFile(dst);
			dos = hdfs.append(new Path(path));
			dos.writeBytes(data + "\r\n");
			dos.close();
			log.info("write hdfs " + path + " successed. ");
		} catch (Exception e) {
			e.printStackTrace();
			log.error("write hdfs " + path + " failed. ", e);
		}
	}

	public static boolean exists(String path) {
		boolean flag = false;
		Path dst = new Path(path);
		try {
			flag = hdfs.exists(dst);
			if (flag) {
				log.info("the " + path + " is exists. ");
			} else {
				log.info("the " + path + " is not exists. ");
			}
		} catch (Exception e) {
			log.error("the " + path + " is not exists. ", e);
		}
		return flag;
	}
	
	public static String read(String path) {
		String content = null;
		Path dst = new Path(path);
		try {
			FSDataInputStream in = hdfs.open(dst);
			OutputStream os = new ByteArrayOutputStream();
			IOUtils.copyBytes(in, os, 4096, true);
			content = os.toString();
			os.close();
			in.close();
			log.info("read hdfs content from " + path + " successed. ");
		} catch (Exception e) {
			log.error("read hdfs content from " + path + " failed. ", e);
		}
		return content;
	}
	
}
