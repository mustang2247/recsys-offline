package com.ganqiang.recsys.util;

import java.util.ArrayList;
import java.util.List;

import com.ganqiang.recsys.hbase.HBaseContext;

public final class Initializer {
	
	private static List<Initializable> initializables = new ArrayList<Initializable>();;

	private static void addInitializer(){
		initializables.add(new Constants());
		initializables.add(new HBaseContext());
		initializables.add(new HdfsHelper());
	}

	public static void setup(){
		addInitializer();
		for(Initializable init : initializables){
			init.init();
		}
	}

}
