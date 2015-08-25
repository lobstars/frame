package com.lobstar.base.gateway;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.lobstar.base.role.MissionBase;
import com.lobstar.manage.IWorkerListener;

public class ParkGateWay extends BaseGateWay<MissionBase> {
	protected Client client;
    public ParkGateWay(String name, String host, int port) {
    	Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true)
                .put("cluster.name", name).build();

        this.client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host, port));
    }

    public void submit(MissionBase visitor) throws InterruptedException {
        visitor.submit(client);
    }

    public void getResponse(MissionBase visitor, IWorkerListener listener) {
        visitor.getResponse(client, 20, 500, listener);
    }

    public Object getResponse(MissionBase visitor) {
        return visitor.getResponse(client, 20, 500);
    }

    public Object getResponse(MissionBase visitor, int tryNum, int sleepTime) {
        return visitor.getResponse(client, tryNum, sleepTime);
    }
    public void close() {
        if (this.client != null) {
            this.client.close();
        }
    }
}
