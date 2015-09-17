package com.lobstar.main.zookeeper;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;

import com.lobstar.base.role.master.Master;
import com.lobstar.config.BuildConfiguration;
import com.lobstar.config.Builder;

public class ZooServer {

	 public static void main(String[] args) throws Exception {

	        Master baseParkKeeper = null;
	        try {
	            baseParkKeeper = new Master("test", new Builder().buildConfig(),false);
	            baseParkKeeper.tryBeKeeper();
	        } catch (Exception e) {
	        	e.printStackTrace();
	            if (baseParkKeeper != null) {
	                baseParkKeeper.close();
	            }
	        }
	    }
}
