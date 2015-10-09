package com.lobstar.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;

import com.lobstar.base.log.XLogger;

public class Builder {
	public static final String ZOOKEEPER_HOST = "zookeeper.host";
	public static final String ZOOKEEPER_SNAPDIR = "zookeeper.snapDir";
	public static final String ZOOKEEPER_LOGDIR = "zookeeper.logDir";
	public static final String ZOOKEEPER_TICK = "zookeeper.tick";
	public static final String ZOOKEEPER_PORT = "zookeeper.port";
	
	public static final String INDEX_HOST = "index.host";
	public static final String INDEX_PORT = "index.port";
	public static final String INDEX_NAME = "index.cluster.name";
	public static final String INDEX_REPLICAS = "index.replicas";
	
	public static final String WINDOW_HOST = "window.host";
	public static final String WINDOW_PORT = "window.port";
	
	public static final String WORK_REMAIN_TIME = "work_remain_time";
	private static Logger LOG = XLogger.getLogger(Builder.class);
	
	private Properties properties;
	
	public Builder buildConfig() throws IOException {
        String file = "taskeeper.yml";
        InputStream stream = null;
        String basePath = Builder.class.getClassLoader().getResource("").getPath();
        File conf = new File(basePath + "/conf/" + file);
        LOG.info("xxx-file: "+conf.getAbsolutePath());
        if (conf.exists()) {
            stream = new FileInputStream(conf);
        } else {
            stream = Builder.class.getClassLoader().getResourceAsStream(file);
        }
        if (stream != null) {
        	properties = new Properties();
        	properties.load(stream);
        	
        }
        return this;
    }
	
	public String getProperties(String key) {
		return properties.getProperty(key);
	}
	
}
