package com.ganqiang.recsys.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.Initializable;
import com.ganqiang.recsys.util.StringUtil;

public class HBaseContext implements Initializable{

	private static final Logger logger = Logger.getLogger(HBaseContext.class);
	public static Configuration config;

	private void initTable(HConnectionController hcc) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(hcc.getConfig());
			for(String tablename : Constants.hbase_tables){
				if (admin.tableExists(tablename)) {
					logger.info("hbase table " + tablename  + " already exists!");
				} else {
					HTableDescriptor tableDesc = new HTableDescriptor(
					        TableName.valueOf(tablename));
					HColumnDescriptor hcol = new HColumnDescriptor(Constants.hbase_column_family);
					hcol.setMaxVersions(1);
					// hcol.setInMemory(true);
					tableDesc.addFamily(hcol);
					tableDesc.setDurability(Durability.ASYNC_WAL);
					admin.createTable(tableDesc);
				}
			}

		} catch (Exception e) {
			logger.error("hbase init failed. ", e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public  void init() {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Constants.hbase_zk_quorum);
		if (!StringUtil.isNullOrBlank(Constants.hbase_master)) {
			config.set("hbase.master", Constants.hbase_master);
		}
		config.set("hbase.zookeeper.property.clientPort", String.valueOf(Constants.hbase_zk_client_port));
		HConnectionController cc = new HConnectionController();
		cc.setConfig(config);
		initTable(cc);
		logger.info("hbase context init finish.");
	}

}
