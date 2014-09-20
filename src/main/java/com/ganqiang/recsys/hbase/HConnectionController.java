package com.ganqiang.recsys.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

public  class HConnectionController {

    private static final Logger logger = Logger.getLogger(HConnectionController.class);

    private Configuration config = null;

    public  void setConfig(Configuration config) {
        this.config = config;
    }

    public Configuration getConfig() {
        return config;
    }

    public  HConnection getHConnection() {
        HConnection con = null;
        try {
            if (null == con || con.isClosed()) {
                con = HConnectionManager.createConnection(config);
            }
        } catch (IOException e) {
            logger.error("Cannot get HBase connection.", e.getCause());
        }
        return con;
    }

    public  void closeConnection() {
        HConnection con = getHConnection();
        try {
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (IOException e) {
            logger.error("Cannot to close HBase connection.", e.getCause());
        }
    }

    public HTableInterface getHTableInterface(String tableName) {
        HConnection con = getHConnection();
        try {
           return con.getTable(tableName);
        } catch (IOException e) {
            logger.error("Cannot to get HTableInterface.", e.getCause());
        }
        return null;
    }

    public void closeTable(HTableInterface table)  {
        try {
            table.close();
        } catch (IOException e) {
            logger.error("Cannot to close HBase table.", e.getCause());
        }
     }

}
