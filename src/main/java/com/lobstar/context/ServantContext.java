package com.lobstar.context;

import java.util.Map;

import com.lobstar.base.exception.TaskeeperRuntimeException;

public class ServantContext {
	private static Map<String,Object> map;
	
	public static ServantContext getInstance() {
		return new ServantContext();
	}
	
	private ServantContext(){
	}

	public void putParam(String key,Object value) throws Exception{
		if(map != null) {
			map.put(key, value);
		}else {
			throw new TaskeeperRuntimeException("map is not init !");
		}
	}
	
	public void putParams(Map<String,Object> params) {
		if(map != null) {
			map.putAll(params);
		}else {
			throw new TaskeeperRuntimeException("map is not init !");
		}
	}
	
	public void setMap(Map<String, Object> map) {
		ServantContext.map = map;
	}
	
	

}
