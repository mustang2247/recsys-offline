package com.ganqiang.recsys.zk;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;

import com.ganqiang.recsys.Recsys;
import com.ganqiang.recsys.util.Constants;

//检测Swift爬虫是否完成一次爬取任务，如果完成则通知hadoop开始进行数据处理
public class SwiftWatcher implements Watcher {

	private static final Logger logger = Logger.getLogger(SwiftWatcher.class);

	public static final int SESSION_TIMEOUT = 30000;
	private ZooKeeper zk;
	private CountDownLatch conSignal = new CountDownLatch(1);
	public static final int RETRY_MAX_COUNT = Constants.zk_retry;

	public static void main(String... args) throws Exception {
		SwiftWatcher wa = new SwiftWatcher();
		while (true) {
			ZooKeeper zk = wa.getConnection(Constants.zk_address);
			zk.exists(Constants.zk_start_flag, true);
			TimeUnit.SECONDS.sleep(300);
		}
	}

	public ZooKeeper getConnection(String host) {
		int retry = 0;
		try {
			zk = new ZooKeeper(host, SESSION_TIMEOUT, this);
			if (States.CONNECTING == zk.getState()) {
				try {
					conSignal.await();
				} catch (InterruptedException e) {
					throw new IllegalStateException(e);
				}
			} else {
				while (retry++ < RETRY_MAX_COUNT) {
					logger.warn("tring to connect " + host
							+ ", retry count is " + retry);
					reConnection(host);
				}
				if (retry == RETRY_MAX_COUNT) {
					logger.error(host + " is not connected.");
				}
			}
		} catch (Exception e) {
			while (retry++ < RETRY_MAX_COUNT) {
				logger.warn("tring to connect, retry count is " + retry, e);
				reConnection(host);
			}
			if (retry == RETRY_MAX_COUNT) {
				logger.error(host + " is not connected.", e);
			}
		}
		return zk;
	}

	public void reConnection(String host) {
		try {
			zk = new ZooKeeper(host, SESSION_TIMEOUT, this);
		} catch (Exception ex) {
			logger.error("retry connection is fault.", ex);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			conSignal.countDown();
		}
		synchronized (this) {
			if (event.getPath() != null
					&& event.getPath().equals(Constants.zk_start_flag)
					&& event.getType() == Event.EventType.NodeCreated) {
				try {
					if (zk.exists(Constants.zk_znode, false) != null) {
						List<String> list = zk.getChildren(Constants.zk_znode,	true);
						if (list != null && list.size() > 0) {
							for (String str : list) {
								zk.delete(Constants.zk_znode + "/" + str, -1);
							}
						}
						zk.delete(Constants.zk_znode, -1);
						logger.info("delete zookeeper dir successful. It begin to start hadoop task...");
						Recsys.runAllTask();
						
					} 
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

}
