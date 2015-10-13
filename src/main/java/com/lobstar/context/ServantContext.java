package com.lobstar.context;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

import com.lobstar.base.exception.TaskeeperRuntimeException;
import com.lobstar.config.Constant;
import com.lobstar.index.QueryTools;

public class ServantContext {
	private Client client;
	private  Map<String,Object> map;
	private  SearchHit searchHit;
	
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
	
	public void updateResponse(Object ret) {
		Map<String,Object> resp = new HashMap<String, Object>();
		resp.put(Constant.WORK_RESPONSE_ASYNC_SYMBOL, ret);
		resp.put(Constant.WORK_DONE_SYMBOL, "true");
		QueryTools.updateIndexData(client,
				searchHit.getIndex(), searchHit.getType(),
				searchHit.getId(), resp);
	}
	
	public void updateException(Exception e) {
		Map<String,Object> resp = new HashMap<String, Object>();
		resp.put(Constant.WORK_EXCEPTION, e.getMessage());
		resp.put(Constant.WORK_DONE_SYMBOL, "error");
		QueryTools.updateIndexData(client,
				searchHit.getIndex(), searchHit.getType(),
				searchHit.getId(), resp);
	}

	public SearchHit getSearchHit() {
		return searchHit;
	}

	public void setSearchHit(SearchHit searchHit) {
		this.searchHit = searchHit;
	}

	public Map<String, Object> getMap() {
		return map;
	}

	public void setMap(Map<String, Object> map) {
		this.map = map;
	}

	public Client getClient() {
		return client;
	}

	public void setClient(Client client) {
		this.client = client;
	}
	
	
	

}
