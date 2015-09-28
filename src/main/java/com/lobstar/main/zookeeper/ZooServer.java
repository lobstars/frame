package com.lobstar.main.zookeeper;


import com.lobstar.base.role.master.Master;
import com.lobstar.config.Builder;

public class ZooServer {

	 public static void main(String[] args) throws Exception {

	        Master baseParkKeeper = null;
	        try {
	            baseParkKeeper = new Master("test", new Builder().buildConfig(),false);
	            baseParkKeeper.work();
	        } catch (Exception e) {
	        	e.printStackTrace();
	            if (baseParkKeeper != null) {
	                baseParkKeeper.close();
	            }
	        }
	    }
}
