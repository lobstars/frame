package com.lobstar.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;

import com.lobstar.base.log.XLogger;

public class Builder {
	public static final String ZOOKEEPER_HOST = "taskeeper.zookeeper.host";
	public static final String ZOOKEEPER_SNAPDIR = "taskeeper.zookeeper.snapDir";
	public static final String ZOOKEEPER_LOGDIR = "taskeeper.zookeeper.logDir";
	public static final String ZOOKEEPER_TICK = "taskeeper.zookeeper.tick";
	public static final String ZOOKEEPER_PORT = "taskeeper.zookeeper.port";
	
	public static final String INDEX_HOST = "taskeeper.index.host";
	public static final String INDEX_PORT = "taskeeper.index.port";
	public static final String INDEX_NAME = "taskeeper.index.cluster.name";
	
	public static final String WINDOW_HOST = "taskeeper.window.host";
	public static final String WINDOW_PORT = "taskeeper.window.port";
	private static Logger LOG = XLogger.getLogger(Builder.class);
	
	private Properties properties;
	
	public Builder buildConfig() throws IOException {
        String file = "taskeeper.yml";
        InputStream stream = null;
        String basePath = BuildConfiguration.class.getClassLoader().getResource("").getPath();
        File conf = new File(basePath + "/conf/" + file);
        LOG.info("xxx-file: "+conf.getAbsolutePath());
        if (conf.exists()) {
            stream = new FileInputStream(conf);
        } else {
            stream = BuildConfiguration.class.getClassLoader().getResourceAsStream(file);
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
