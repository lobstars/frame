package com.lobstar.base.gateway;

import com.lobstar.base.exception.TaskeeperRuntimeException;
import com.lobstar.base.role.Mission;
import com.lobstar.manage.IWorkerListener;

public class NettyGateWay{

	private String host;
	private int port;
	
	
    public NettyGateWay(String host,int port) {
    	this.host = host;
    	this.port = port;
    }
    public NettyGateWay() {
	}

    public void submit(Mission visitor) throws Exception {
    	if(visitor.getHost() == null && this.host != null)
    	{
    		visitor.setHost(this.host);
    	}
    	if(visitor.getPort() == 0 && this.port != 0)
    	{
    		visitor.setPort(this.port);
    	}
        visitor.submit();
    }

    public void getResponse(Mission visitor, IWorkerListener<Mission> listener) {
    	if(visitor.isSubmit())
    	{
    		visitor.getResponse(20, 500, listener);    		
    	}
    	else{
    		throw new TaskeeperRuntimeException("get response from no-submit visitor!");
    	}
    }

    public Object getResponse(Mission visitor, int tryNum, int sleepTime) throws Exception {
    	if(visitor.isSubmit())
    	{
    		return visitor.getResponse(tryNum, sleepTime);    		
    	}else{
    		throw new TaskeeperRuntimeException("get response from no-submit visitor!");
    	}
    }

}
