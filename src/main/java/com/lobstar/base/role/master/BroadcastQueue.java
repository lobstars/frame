package com.lobstar.base.role.master;


import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;

import com.lobstar.base.log.XLogger;
import com.lobstar.config.Constant;
import com.lobstar.index.QueryTools;
import com.lobstar.queryer.QueryGenerator;

public class BroadcastQueue {
	private static final Logger logger = XLogger.getLogger(Master.class);
	private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
	private ObjectMapper mapper = new ObjectMapper();
	private Client client;
	public BroadcastQueue(Client client){
		this.client = client;
		
	}
	
	public void start() {
		executor.execute(new SeekBroadcastTask());
	}
	
	enum BroadcastStatus {
		establish,done,error
	}
	
	class BroadcastTask {
		private String key;
		private String index;
		private Map<String,Object> params;
		private Map<String,BroadcastStatus> tasks;
		
		public BroadcastTask() {
			tasks = new ConcurrentHashMap<String, BroadcastQueue.BroadcastStatus>();
			setIndex(QueryTools.getDailyIndex());
		}
		
		public String getKey() {
			return key;
		}
		public void setKey(String key) {
			this.key = key;
		}
		public Map<String, Object> getParams() {
			return params;
		}
		public void setParams(Map<String, Object> params) {
			this.params = params;
		}
		public Map<String,BroadcastStatus> getTasks() {
			return tasks;
		}
		public void setTasks(Map<String,BroadcastStatus> tasks) {
			this.tasks = tasks;
		}

		public String getIndex() {
			return index;
		}

		public void setIndex(String index) {
			this.index = index;
		}
		
	}
	
	private int queueSize = 10;
	

	private BlockingQueue<BroadcastTask> queue = new ArrayBlockingQueue<BroadcastTask>(queueSize);
	
	private Map<String,BroadcastTask> broadcastStatusMap = new ConcurrentHashMap<String, BroadcastTask>();
	
	public String add(Map<String,Object> source) {
		String key = Constant.WORK_DOMAIN_BROADCAST+UUID.randomUUID().toString();
		BroadcastTask broadcastTask = new BroadcastTask();
		broadcastTask.setParams(source);
		broadcastTask.setKey(key);
		queue.offer(broadcastTask);
		broadcastStatusMap.put(key,broadcastTask);
		return key;
	}
	
	public String response(String key) {
		return response(key,200,50);	
	}
	
	public Map<String,BroadcastStatus> status(String key) {
		BroadcastTask task = broadcastStatusMap.get(key);
		return task.getTasks();
	}
	
	
	public String response(String key,int wait,int tick) {
		BroadcastTask task = broadcastStatusMap.get(key);
		String query = "_id=\""+key+"\"";
		for(int ttl =0;ttl<tick;ttl++) {
			try {
				SearchResponse response = client.prepareSearch(task.getIndex()).setQuery(QueryGenerator.generateQuery(query)).get();
				SearchHit[] hits = response.getHits().getHits();
				for (SearchHit searchHit : hits) {
					Object ret = searchHit.getSource().get(Constant.WORK_RESPONSE_SYMBOL);
					if(ret != null){
						try{
							Map<String,Object> retMap = mapper.readValue(ret.toString(), Map.class);
							ret = retMap.get(Constant.WORK_RESPONSE_SYMBOL);
						}catch(Exception e){
							logger.error("",e);
						}
						String type = searchHit.getType();
						if(ret.equals("success")) {
							task.getTasks().put(type, BroadcastStatus.done);							
						}else {
							task.getTasks().put(type, BroadcastStatus.error);
						}
					}
				}
				if(task.getTasks().containsValue(BroadcastStatus.establish)) {
					Thread.sleep(wait);
					continue;
				}
				return "done";
				
			}catch(Exception e){
				continue;
			}			
		}
		
		return "timeout";
	}
	
	private BroadcastTask fetchBroadcastTask() throws InterruptedException {
		return queue.take();
	}
	
	
	private class SeekBroadcastTask implements Runnable {
		
		private ScheduledExecutorService pool = new ScheduledThreadPoolExecutor(10);
		@Override
		public void run() {
			while(true) {
				try {
					BroadcastTask task = fetchBroadcastTask();
					distributeTasks(task);
				}catch(Exception e){
					logger.error("",e);
				}
			}
		}
		
		private void distributeTasks(BroadcastTask task) {
			Map<String, Object> params = task.getParams();
			if(params == null) {
				broadcastStatusMap.put(task.getKey(), task);
				return;
			}
			Object domain = params.get(Constant.WORK_DOMAIN_SYMBOL);
			CopyOnWriteArrayList<String> workList = null;
			
			//视作全worker广播
			if(domain == null) {
				workList = ServantGroup.getWorkerSet();
			}else {
				workList = ServantGroup.getDomainWorkerMap().get(domain);
			}
			for (String type : workList) {
				task.getTasks().put(type, BroadcastStatus.establish);
				pool.execute(new BroadcastHandler(type,task));
			}
		}
	}
	
	class BroadcastHandler implements Runnable {
		private String index;
		private String type;
		private Map<String,Object> data;
		private String id;
		public BroadcastHandler(String type,BroadcastTask task) {
			this.index = task.getIndex();
			this.id = task.getKey();
			this.type = type;
			this.data = task.getParams();
		}
		@Override
		public void run() {
        	try{
        		QueryTools.insertIndex(client, index, type, id,data);                		
        	}
        	catch(Exception e)
        	{
        		if(!QueryTools.isIndexExist(client, index))
        		{
        			QueryTools.createIndex(client, index);
        			QueryTools.insertIndex(client, index, type, id,data); 
        		}
        	}
		}
	}

}
