package com.jianfeitech.test.main;

import java.io.IOException;
import java.util.UUID;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.lobstar.base.role.master.Master;
import com.lobstar.config.BuildConfiguration;

public class MasterTest {
	public static void main(String[] args) throws IOException {

        Master baseParkKeeper = null;
        try {
        	
        	Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true)
                    .put("cluster.name", "x-factory").build();
        	
            baseParkKeeper = new Master("t-master","127.0.0.1:12888",settings,"127.0.0.1",9300);
       
            baseParkKeeper.initTicket(null, 10888);
            baseParkKeeper.tryBeKeeper();
        } catch (Exception e) {
            if (baseParkKeeper != null) {
                baseParkKeeper.close();
            }
        }
    }
}
