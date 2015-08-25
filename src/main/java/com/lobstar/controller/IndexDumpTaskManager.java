package com.lobstar.controller;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;

import com.lobstar.base.exception.TaskeeperRuntimeException;
import com.lobstar.base.log.XLogger;
import com.lobstar.base.role.master.Master;
import com.lobstar.config.Constant;
import com.lobstar.index.QueryTools;

public class IndexDumpTaskManager extends TimerTask{
	private static final Logger logger = XLogger.getLogger(Master.class);
	private Client client;
	
	
	private static final long THREE_DAYS = 86400000*3;
	private static final long HOLD_TIME = THREE_DAYS;
	
	
	public IndexDumpTaskManager(Client client) {
		this.client = client;
		initDumpIndex();
	}
	
	
	
	@Override
	public void run() {
		try{
			logger.info("start to dump history");
			String[] store = getStoreIndices();
			logger.info("all indices:"+Arrays.asList(store));
			for (String index : store) {
					QueryTools.backupIndex(client, index);
					if(!waitForDelete(index)) {
						throw new TaskeeperRuntimeException("can not delete index: "+index);
					}
			}
			
		}catch(Exception e)
		{
			logger.error("",e);
		}
		
		
	}
	
	
	private void initDumpIndex(){
		if(!QueryTools.isIndexExist(client, Constant.DUMP_INDEX_NAME)){
			QueryTools.createIndex(client, Constant.DUMP_INDEX_NAME);
		}
	}
	
	
	private boolean waitForDelete(String index){
		new DeleteIndexRequestBuilder(client.admin().indices(), index).execute();
		int ttl = 0;
		while(QueryTools.isIndexExist(client, index)){
			try {
				if(ttl > 50) {
					return false;
				}
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error(e.getMessage(),e);
			}
			finally{
				ttl++;
			}
		}
		return true;
	}
	
	private String[] getStoreIndices() throws Exception{
		Set<String> allIndices = getAllIndices();
		
		Set<String> storeIndices = new HashSet<String>();
		
		for (String index : allIndices) {
			if(index.matches("[0-9]+")){
				long indexTime = Long.parseLong(index);
				long nowTime = new Date().getTime();
				
				if(nowTime - indexTime > HOLD_TIME) {
					storeIndices.add(index);
				}
			}
		}
		
		String[] indices = new String[storeIndices.size()];
		
		indices = storeIndices.toArray(indices);
		
		return indices;
	}
	
	private Set<String> getAllIndices() throws Exception{
		
		return client.admin().indices().stats(new IndicesStatsRequest()).get().getIndices().keySet();
		
	}

}
