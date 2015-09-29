package com.lobstar.controller;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;

import com.lobstar.base.exception.TaskeeperRuntimeException;
import com.lobstar.base.log.XLogger;
import com.lobstar.base.role.master.Master;
import com.lobstar.config.Constant;
import com.lobstar.index.QueryTools;
import com.lobstar.queryer.QueryGenerator;
import com.lobstar.utils.Utils;

public class IndexDumpTaskManager extends TimerTask{
	private static final Logger logger = XLogger.getLogger(Master.class);
	private Client client;
	
	
	private static final long ONE_DAY = 86400000;
	private static final long HOLD_TIME = ONE_DAY;
	
	private int remainTime = 5;
	
	
	public IndexDumpTaskManager(Client client,int remainTime) {
		this.client = client;
		this.remainTime = remainTime;
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
		try {
			removeExpireDatas();
		}catch(Exception e) {
			throw new TaskeeperRuntimeException("remove index failed :"+e);
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
	
	private void removeExpireDatas() throws Exception{
		
		Long now = Utils.getSystemTime();
		Long targetTime = getDeleteTimeLine(now);
		String cmd = Utils.contact(Constant.VISITOR_TIME_SYMBOL,"=",targetTime);
		SearchResponse response = client.prepareSearch(Constant.DUMP_INDEX_NAME).setQuery(QueryGenerator.generateQuery(cmd))
		.setScroll(new TimeValue(60000))
		.setSearchType(SearchType.SCAN)
		.setSize(10000)
		.get();
		
		BulkProcessor processor = QueryTools.buildProcessor(client,10000);
		while (true) {
			SearchHit[] hits = response.getHits().getHits();

			for (SearchHit searchHit : hits) {
				
				processor.add(new DeleteRequest(searchHit.getIndex(), searchHit.getType(), searchHit.getId()));
			}
			logger.info("delete index ..." + hits.length);
			response = client.prepareSearchScroll(response.getScrollId())
					.setScroll(new TimeValue(60000)).get();

			if (response.getHits().getHits().length == 0) {
				break;
			}
		}
		
	}

	private long getDeleteTimeLine(Long now) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(now);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		calendar.add(Calendar.DATE, -remainTime);
		return calendar.getTimeInMillis();
	}
	
	
}
