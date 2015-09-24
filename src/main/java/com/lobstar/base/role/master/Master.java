package com.lobstar.base.role.master;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;

import com.lobstar.base.exception.TaskeeperBuilderException;
import com.lobstar.base.log.XLogger;
import com.lobstar.base.role.ServantEquipment;
import com.lobstar.base.service.status.StatusServant;
import com.lobstar.config.BuildConfiguration;
import com.lobstar.config.Builder;
import com.lobstar.config.Constant;
import com.lobstar.controller.IndexDumpTaskManager;
import com.lobstar.index.QueryTools;
import com.lobstar.manage.IDistributionHandler;
import com.lobstar.manage.IKeeperHandler;
import com.lobstar.queryer.QueryGenerator;

public class Master extends ServantEquipment {
	private static final String DOMAIN_ALL = "default";
	private static final String WORKER_SYMBOL = "/workers";
	private static final String MASTER_SYBOL = "/master";
	private static final Logger logger = XLogger.getLogger(Master.class);
	private LeaderSelector leaderSelector;
	private BroadcastQueue broadcastQueue;

	private CopyOnWriteArrayList<String> workerSet = ServantGroup
			.getWorkerSet();

	private Map<String, CopyOnWriteArrayList<String>> domainWorkerMap = ServantGroup
			.getDomainWorkerMap();

	private MissionWindow ticketWindow;

	private long tickTime = 3;
	//
	private int searchNum = Constant.MASTER_POLL_INTERVAL;

	private String ticketHost = Constant.LOCAL_ADDRESS;

	private int ticketPort = Constant.TICKET_PORT;

	private Timer dumpTimer = new Timer("dumper");
	
	private String indexRefreshInterval = "1s";
	private int indexReplicas = 0;

	private ScheduledExecutorService poolExecutor = (ScheduledThreadPoolExecutor) Executors
			.newScheduledThreadPool(1);

	private IDistributionHandler distributionHandler = new IDistributionHandler() {

		@Override
		public String distribution(Map<String, Object> source,
				CopyOnWriteArrayList<String> workerSet,
				Map<String, CopyOnWriteArrayList<String>> domainWorkerMap) {
			if (source != null) {
				Object domain = source.get(Constant.WORK_DOMAIN_SYMBOL);
				 Object workName = source.get(Constant.WORK_SERVANT_NAME);
	                if(workName != null) {
	                	String name = workName.toString();
	                	for (String worker : workerSet) {
							if(name.equals(worker)) {
								return worker;
							}
						}
	                }
				if (domain != null) {
					if (domainWorkerMap.containsKey(domain)
							&& domainWorkerMap.get(domain).size() > 0) {
						int label = Math
								.abs(source.hashCode() % domainWorkerMap.get(domain).size());
						String type = domainWorkerMap.get(domain).get(label);
						return type;
					}
				}
			}
			CopyOnWriteArrayList<String> customServants = domainWorkerMap.get(Constant.WORK_CUSTOM_DOMAIN_DEFALUT);
			if(customServants != null) {
				int label = Math.abs(source.hashCode() % domainWorkerMap.get(Constant.WORK_CUSTOM_DOMAIN_DEFALUT).size());
				String type = domainWorkerMap.get(Constant.WORK_CUSTOM_DOMAIN_DEFALUT).get(label);
				return type;				
			}
			return null;
		}
	};

	private IKeeperHandler handler = new IKeeperHandler() {
		@Override
		public void stateChanged(CuratorFramework client,
				ConnectionState newState) {
		}

		@Override
		public void takeLeadership(CuratorFramework client) throws Exception {
			logger.info(getId() + " --->take leader!");
			PathChildrenCache workerCache = null;

			try {
				workerCache = new PathChildrenCache(client, WORKER_SYMBOL, true);
				ticketWindow = new MissionWindow(Master.this, ticketHost,
						ticketPort);
				workerCache.getListenable().addListener(
						new PathChildrenCacheListener() {
							@Override
							public void childEvent(CuratorFramework client,
									PathChildrenCacheEvent event)
									throws Exception {
								if (event.getType() == Type.CHILD_ADDED) {
									// add a worker
									String path = event.getData().getPath();
									String workerName = path.substring(path
											.lastIndexOf("/") + 1);
									String workDomain = new String(event
											.getData().getData(), Charset
											.forName(Constant.GLOBAL_CHARSET));
									if (workDomain == null
											|| workDomain.trim().equals("")
											|| workDomain.toLowerCase().equals(
													DOMAIN_ALL)) {
										workDomain = DOMAIN_ALL;
									}
									logger.info(workerName + " join in;domain:"
											+ workDomain);
									workerSet.add(workerName);

									if (domainWorkerMap.get(workDomain) == null) {
										CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList<String>();
										list.add(workerName);
										domainWorkerMap.put(workDomain, list);
									} else {
										domainWorkerMap.get(workDomain).add(
												workerName);
									}

									if (!workDomain.startsWith("_")) {
										if(domainWorkerMap.get(Constant.WORK_CUSTOM_DOMAIN_DEFALUT) == null) {
											CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList<String>();
											list.add(workerName);
											domainWorkerMap.put(Constant.WORK_CUSTOM_DOMAIN_DEFALUT, list);
										}else {
											domainWorkerMap.get(Constant.WORK_CUSTOM_DOMAIN_DEFALUT).add(
													workerName);
										}
									}
								}
								if (event.getType() == Type.CHILD_REMOVED) {
									// remove a worker
									String path = event.getData().getPath();
									String domainName = new String(event
											.getData().getData(), Charset
											.forName(Constant.GLOBAL_CHARSET));
									String workerName = path.substring(path
											.lastIndexOf("/") + 1);
									workerSet.remove(workerName);
									if (domainWorkerMap.get(domainName) != null) {
										domainWorkerMap.get(domainName).remove(
												workerName);
									}
									logger.info(workerName + " leave");
									Map<String, Object> extraData = new HashMap<String, Object>();
									extraData.put(Constant.WORK_UNDO_SYMBOL,
											"true");
									extraData.put(Constant.WORK_ASSIGN_SYMBOL,
											"true");
									extraData.put(Constant.WORK_ASSIGN_SYMBOL,
											"false");
									QueryTools.moveIndexData(
											getRepositoryClient(),
											QueryTools.getDailyIndex(),
											workerName,
											Constant.WORK_NOT_ASSIGN_TYPE,
											QueryGenerator
													.generateQuery(Constant.WORK_DONE_SYMBOL
															+ "=false"),
											extraData);
								}
								// ticketWindow.updateWorkerMap(workerSet,
								// domainWorkerMap);
							}
						});
				workerCache.start();
				initIndex();
				ticketWindow.open(getRepositoryClient());
				broadcastQueue = new BroadcastQueue(getRepositoryClient());
				broadcastQueue.start();
				logger.info("master--->" + Master.this.getId()
						+ "   ->   init!");
				
				StatusServant.startService();
				logger.info("manage servant init -----> status servant start");
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				if (workerCache != null) {
					workerCache.close();
				}
			}
		}
	};

	private CountDownLatch closeLatch = new CountDownLatch(1);

	/* -------------------------------Master()--------------------------------------------*/
	
	public Master() {
	}
	
	public Master(String name,Builder builder,boolean candidate) throws Exception{
		if(!candidate) {
			File snapDir = new File(builder.getProperties(Builder.ZOOKEEPER_SNAPDIR));
			File logDir = new File(builder.getProperties(Builder.ZOOKEEPER_LOGDIR));
			
			if(!snapDir.exists()) {
				snapDir.mkdirs();
			}
			if(!logDir.exists()) {
				logDir.mkdirs();
			}
			int tickTime = 1000;
			String tick = builder.getProperties(Builder.ZOOKEEPER_TICK);
			if(tick != null) {
				tickTime = Integer.parseInt(tick);
			}
			String host = builder.getProperties(Builder.ZOOKEEPER_HOST);
			int port = Integer.parseInt(builder.getProperties(Builder.ZOOKEEPER_PORT));
			ZooKeeperServer server = new ZooKeeperServer(snapDir, logDir, tickTime);
			ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
            cnxnFactory.configure(new InetSocketAddress(host,port),
                    60);
            cnxnFactory.startup(server);
		}
		setId(name);
		if(builder.getProperties(Builder.INDEX_REFRESH_INTERVAL) != null) {
			this.indexRefreshInterval = builder.getProperties(Builder.INDEX_REFRESH_INTERVAL);			
		}
		if(builder.getProperties(Builder.INDEX_REPLICAS) != null) {
			this.indexReplicas = Integer.parseInt(builder.getProperties(Builder.INDEX_REPLICAS));
		}
        Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true)
                .put("cluster.name", builder.getProperties(Builder.INDEX_NAME)).build();
        setRepositoryClient( new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(builder.getProperties(Builder.INDEX_HOST), Integer.parseInt(builder.getProperties(Builder.INDEX_PORT)))));
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(100, 1);
        setZookeeperClient(CuratorFrameworkFactory.newClient(builder.getProperties(Builder.ZOOKEEPER_HOST) + ":" + builder.getProperties(Builder.ZOOKEEPER_PORT),
                retryPolicy));
        getZookeeperClient().start();
	}

	public Master(String name, String connectString, Settings settings,
			String host, int port) {
		super(name, connectString, settings, host, port);
	}

	public Master(String name, BuildConfiguration config) {
		super(name, config);
		initTicket(config.getTicketHost(), config.getTicketPort());
	}

	public void initTicket(String ticketHost, int ticketPort) {
		this.ticketHost = ticketHost;
		this.ticketPort = ticketPort;
	}

	public void tryBeKeeper() {
		String uid = null;
		if (ticketHost == null) {
			try {
				uid = MASTER_SYBOL + "/"
						+ InetAddress.getLocalHost().getHostAddress() + "@"
						+ ticketPort;
			} catch (UnknownHostException e) {
				logger.error(e.getMessage(), e);
				throw new TaskeeperBuilderException("can not get host ip!");
			}
		} else {
			uid = MASTER_SYBOL + "/" + ticketHost + "@" + ticketPort;
		}
		logger.info("master uid:" + uid + "try to be a master");
		leaderSelector = new LeaderSelector(getZookeeperClient(), uid,
				new LeaderSelectorListener() {
					@Override
					public void stateChanged(CuratorFramework client,
							ConnectionState newState) {
						handler.stateChanged(client, newState);
					}

					@Override
					public void takeLeadership(CuratorFramework client)
							throws Exception {
						handler.takeLeadership(client);
						poolExecutor.scheduleWithFixedDelay(new SeekWork(), 0,
								tickTime, TimeUnit.SECONDS);
						poolExecutor.scheduleWithFixedDelay(
								new IndexBuilderTimer(), 0, tickTime,
								TimeUnit.HOURS);
						dumpTimer.schedule(new IndexDumpTaskManager(
								getRepositoryClient()), findDumpDate(),
								1000 * 60 * 60 * 24);
						try {
							closeLatch.await();
						} catch (InterruptedException e) {
							logger.error(e.getMessage(), e);
						}
					}
				});

		leaderSelector.setId(getId());
		leaderSelector.autoRequeue();
		leaderSelector.start();
		try {
			closeLatch.await();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}

	protected class SeekWork extends Thread {
		@Override
		public void run() {
			try {
				String dailyIndex = QueryTools.getDailyIndex();

				if (!QueryTools.isIndexExist(getRepositoryClient(), dailyIndex)) {
					Settings settings = ImmutableSettings.settingsBuilder()
							.put("number_of_replicas", Master.this.indexReplicas)
							.put("refresh_interval",Master.this.indexRefreshInterval)
							.build();
					QueryTools.createDailyIndex(getRepositoryClient(),settings);
				}

				SearchResponse response = QueryTools.searchIndexAndType(
						getRepositoryClient(), dailyIndex,
						Constant.WORK_NOT_ASSIGN_TYPE, searchNum);
				SearchHit[] hits = response.getHits().getHits();

				if (hits.length != 0) {
					if (workerSet.size() == 0) {
						logger.info("no worker !");
						return;
					}
				}

				for (SearchHit searchHit : hits) {
					String type = distributionHandler.distribution(
							searchHit.getSource(), workerSet, domainWorkerMap);
					Map<String, Object> source = searchHit.getSource();
					source.put(Constant.WORK_ASSIGN_SYMBOL, "true");
					QueryTools.moveIndexData(getRepositoryClient(), dailyIndex,
							Constant.WORK_NOT_ASSIGN_TYPE, type,
							searchHit.getId(), source);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}

		}
	}

	private class IndexBuilderTimer extends Thread {
		@Override
		public void run() {
			initIndex();
		}
	}

	protected void initKeeper() {
		try {
			getZookeeperClient().create().forPath(WORKER_SYMBOL);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

		try {
			getZookeeperClient().create().forPath(MASTER_SYBOL);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void addKeepHandler(IKeeperHandler handler) {
		this.handler = handler;
	}

	public void addDistributionHandler(IDistributionHandler handler) {
		this.distributionHandler = handler;
	}

	public List<String> getWorkerSet() {
		return this.workerSet;
	}

	public Map<String, CopyOnWriteArrayList<String>> getDomainWorkerMap() {
		return domainWorkerMap;
	}

	private void initIndex() {
		String dailyIndex = QueryTools.getDailyIndex();

		if (!QueryTools.isIndexExist(getRepositoryClient(), dailyIndex)) {
			Settings settings = ImmutableSettings.settingsBuilder()
					.put("number_of_replicas", this.indexReplicas)
					.put("refresh_interval",this.indexRefreshInterval)
					.build();
			QueryTools.createDailyIndex(getRepositoryClient(),settings);
		}

		String nextIndex = QueryTools.getNextDayIndex();

		if (!QueryTools.isIndexExist(getRepositoryClient(), nextIndex)) {
			Settings settings = ImmutableSettings.settingsBuilder()
					.put("number_of_replicas", this.indexReplicas)
					.put("refresh_interval",this.indexRefreshInterval)
					.build();
			QueryTools.createIndex(getRepositoryClient(), nextIndex,settings);
		}
	}

	private Date findDumpDate() {
		Calendar calendar = Calendar.getInstance();
		if (calendar.get(Calendar.HOUR_OF_DAY) > 2) {
			calendar.add(Calendar.DATE, 1);
			calendar.set(Calendar.HOUR_OF_DAY, 2);
		} else {
			calendar.set(Calendar.HOUR_OF_DAY, 2);
		}

		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);

		return calendar.getTime();
	}

	public BroadcastQueue getBroadcastQueue() {
		return broadcastQueue;
	}

}
