package com.lobstar.base.role.mission;

import java.util.HashMap;
import java.util.Map;

import com.lobstar.config.Constant;

public class MissionReport {
	
	private String host;
	private Integer port;
	
	private Map<String,Object> result;
	private String exception;
	private Map<String,Object> metadata;
	private Map<String,Object> asyncResult;
	
	private boolean complete = false;
	private boolean error = false;
	private boolean async;
	
	public MissionReport getAsyncReport() throws Exception{
		Map<String,Object> param = new HashMap<String, Object>();
		param.putAll(result);
		MissionReport report = new Mission(host,port).setData(param)
				.addParam("_domain", "_status")
				.addParam("_action", "getAsyncResponse")
				.submit().reportGet();
		this.asyncResult = (Map<String, Object>) report.getResult().get(Constant.WORK_RESPONSE_ASYNC_SYMBOL);
		return this;
	}
	
	
	public Map<String, Object> getResult() {
		return result;
	}
	public void setResult(Map<String, Object> result) {
		this.result = result;
	}
	public String getException() {
		return exception;
	}
	public void setException(String exception) {
		this.exception = exception;
	}
	public Map<String, Object> getMetadata() {
		return metadata;
	}
	public void setMetadata(Map<String, Object> metadata) {
		this.metadata = metadata;
	}
	public boolean isComplete() {
		return complete;
	}
	public void setComplete(boolean complete) {
		this.complete = complete;
	}
	public boolean isError() {
		return error;
	}
	public void setError(boolean error) {
		this.error = error;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public Integer getPort() {
		return port;
	}
	public void setPort(Integer port) {
		this.port = port;
	}
	public Map<String,Object> getAsyncResult() {
		return asyncResult;
	}
	public void setAsyncResult(Map<String,Object> asyncResult) {
		this.asyncResult = asyncResult;
	}
	public boolean isAsync() {
		return async;
	}
	public void setAsync(boolean async) {
		this.async = async;
	}
	
	
	
	
}
