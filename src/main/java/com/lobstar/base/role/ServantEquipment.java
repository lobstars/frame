package com.lobstar.base.role;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.lobstar.base.exception.TaskeeperBuilderException;
import com.lobstar.config.BuildConfiguration;
import com.lobstar.config.Builder;
import com.lobstar.config.Constant;

public class ServantEquipment implements Closeable {

    private Client repositoryClient;
    private CuratorFramework zookeeperClient;

    private String id;
    
    private List<String> systemServants = Arrays.asList(new String[]{
    		Constant.SYSTEM_SERVANT_MANAGER,Constant.SYSTEM_SERVANT_STATUS
    });

    public ServantEquipment() {
    }

    @SuppressWarnings("resource")
    public ServantEquipment(String id, String connectString, Settings settings, String host, int port) {
        try {
            setId(id);

            repositoryClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host,
                    port));
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(100, 1);
            zookeeperClient = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
            zookeeperClient.start();
        } catch (Exception e) {
            try {
                close();
            } catch (IOException e1) {
                throw new TaskeeperBuilderException("init error!", e1);
            }
            throw new TaskeeperBuilderException("init error!", e);
        }
    }

    @SuppressWarnings("resource")
    public ServantEquipment(String id, BuildConfiguration config) {
        setId(id);
        Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true)
                .put("cluster.name", config.getEsName()).build();
        repositoryClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(config
                .getEsHost(), config.getEsPort()));
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(100, 1);
        zookeeperClient = CuratorFrameworkFactory.newClient(config.getZooHost() + ":" + config.getZooPort(),
                retryPolicy);
        zookeeperClient.start();
    }
    
    public ServantEquipment(String id , Builder builder) {
    	setId(id);
        Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true)
                .put("cluster.name", builder.getProperties(Builder.INDEX_NAME)).build();
        repositoryClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(builder.getProperties(Builder.INDEX_HOST), Integer.parseInt(builder.getProperties(Builder.INDEX_PORT))));
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(100, 1);
        zookeeperClient = CuratorFrameworkFactory.newClient(builder.getProperties(Builder.ZOOKEEPER_HOST) + ":" + builder.getProperties(Builder.ZOOKEEPER_PORT),
                retryPolicy);
        zookeeperClient.start();
    }
    

    @Override
    public void close() throws IOException {
        if (repositoryClient != null) {
            repositoryClient.close();
        }
        if (zookeeperClient != null) {
            zookeeperClient.close();
        }
    }

    public Client getRepositoryClient() {
        return repositoryClient;
    }

    public void setRepositoryClient(Client repositoryClient) {
        this.repositoryClient = repositoryClient;
    }

    public CuratorFramework getZookeeperClient() {
        return zookeeperClient;
    }

    public void setZookeeperClient(CuratorFramework zookeeperClient) {
        this.zookeeperClient = zookeeperClient;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
    	if(id.contains("_")) {
    		if(!systemServants.contains(id)) {
    			throw new TaskeeperBuilderException("rule name valid");    			    			
    		}
    	}
        this.id = id;
    }

}
