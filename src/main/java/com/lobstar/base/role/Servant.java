package com.lobstar.base.role;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;

import com.lobstar.base.exception.ExceptionTools;
import com.lobstar.base.exception.TaskeeperRuntimeException;
import com.lobstar.base.log.XLogger;
import com.lobstar.config.BuildConfiguration;
import com.lobstar.config.Constant;
import com.lobstar.context.ServantContext;
import com.lobstar.index.QueryTools;
import com.lobstar.manage.IServantHandler;
import com.lobstar.queryer.QueryGenerator;

public class Servant extends ServantEquipment {

	private static final Logger logger = XLogger.getLogger(Servant.class);

	private int waitMs = Constant.SERVANT_POLL_INTERVAL;

	private Executor handleExecutor;

	private Executor waitExecutor;

	private BlockingQueue<SearchHit> queue;

	private int queueSize = Constant.SERVANT_TASK_QUEUE_SIZE;

	private int executorSize = Constant.SERVANT_HANDLE_WORK_THREAD_NUM;

	private String domain = "";

	private Queue<String> cacheSet;

	private IServantHandler handler = new IServantHandler() {

		@Override
		public Object doAssignWorks(ServantContext sc,
				Map<String, Object> source) {

			logger.info(source.toString());
			return "success";
		}
	};

	private ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(
			3);

	public Servant() {
		initField();
	}

	public Servant(String id, String connectString, Settings settings,
			String host, int port) {
		super(id, connectString, settings, host, port);
		initField();
		init();
	}

	public Servant(String id, BuildConfiguration config) {
		super(id, config);
		initField();
		init();
	}

	private void initField() {
		this.handleExecutor = Executors
				.newFixedThreadPool(Constant.SERVANT_HANDLE_WORK_THREAD_NUM);
		this.waitExecutor = Executors.newFixedThreadPool(executorSize);
		queue = new ArrayBlockingQueue<SearchHit>(queueSize);
		cacheSet = new ConcurrentLinkedQueue<String>();
	}

	public void joinPark() {
		try {
			this.getZookeeperClient()
					.create()
					.withMode(CreateMode.EPHEMERAL)
					.forPath(
							"/workers/" + getId(),
							domain.getBytes(Charset
									.forName(Constant.GLOBAL_CHARSET)));
			this.getZookeeperClient().getConnectionStateListenable()
					.addListener(new ZKConnectionListener(), poolExecutor);
			logger.info(getId() + " --->start");
			poolExecutor.scheduleWithFixedDelay(new HandleWork(), 1, waitMs,
					TimeUnit.MILLISECONDS);
			poolExecutor.execute(new TakeWork());
			start();
		} catch (Exception e) {
			logger.info("", e);
			throw new TaskeeperRuntimeException("cannot jion park");
		}
	}

	private class ZKConnectionListener implements ConnectionStateListener {

		@Override
		public void stateChanged(CuratorFramework client,
				ConnectionState newState) {

			logger.info("zk connection state changed : " + newState);
			if (newState == ConnectionState.LOST) {
				while (true) {
					try {
						Servant.this.getZookeeperClient()
								.blockUntilConnected();
						Servant.this
								.getZookeeperClient()
								.create()
								.creatingParentsIfNeeded()
								.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
								.forPath(
										"/workers/" + getId(),
										domain.getBytes(Charset
												.forName(Constant.GLOBAL_CHARSET)));
						break;

					} catch (InterruptedException e) {
						logger.error(
								"ZKConnectionListener thread interrupted !", e);
						break;
					} catch (Exception e) {
						logger.error("ZKConnectionListener reconnect error !",
								e);
					}
				}
			}
		}

	}

	public class HandleWork implements Runnable {
		@Override
		public void run() {
			try {
				StringBuffer cmd = new StringBuffer();
				String dailyIndex = QueryTools.getDailyIndex();
				cmd.append(Constant.WORK_DONE_SYMBOL);
				cmd.append("=false");
				SearchResponse response = QueryTools.searchIndexAndType(
						getRepositoryClient(), dailyIndex, getId(),
						QueryGenerator.generateQuery(cmd.toString()));
				SearchHit[] hits = response.getHits().getHits();

				for (SearchHit searchHit : hits) {
					StringBuffer uid = new StringBuffer();
					uid.append(searchHit.getIndex());
					uid.append(searchHit.getType());
					uid.append(searchHit.getId());
					if (!cacheSet.contains(uid.toString())) {
						queue.offer(searchHit);
						cacheSet.offer(uid.toString());
					}
				}

				// executor.execute(new DealWorkThread(searchHit));
				// queue
			} catch (Exception e) {
				logger.error("", e);
				throw new TaskeeperRuntimeException(e);
			}
		}
	}

	private class TakeWork extends Thread {
		@Override
		public void run() {
			while (true) {
				try {
					SearchHit searchHit = queue.take();
					handleExecutor.execute(new DealWorkThread(searchHit));
				} catch (Exception e) {
					logger.error("unhandle exception",e);
				}
			}
		}
	}

	private class DealWorkThread extends Thread {

		private SearchHit searchHit;

		public DealWorkThread(SearchHit searchHit) {
			this.searchHit = searchHit;
		}

		@Override
		public void run() {
			if (searchHit != null) {
				Map<String, Object> data = searchHit.getSource();
				Map<String, Object> map = new HashMap<String, Object>();
				ServantContext sct = ServantContext.getInstance();
				sct.setMap(map);
				Long nowTime = System.currentTimeMillis();
				Object visitTimeObj = data
						.get(Constant.VISITOR_TIME_SYMBOL);
				Long visitTime = nowTime + 1;
				if (visitTimeObj != null) {
					String visitTimeStr = data.get(
							Constant.VISITOR_TIME_SYMBOL).toString();
					visitTime = Long.parseLong(visitTimeStr);
				}
				Long timeSpan = nowTime - visitTime;
				try {
					if (data.get("__testConnect") != null) {
						map.put(Constant.WORK_DONE_SYMBOL, "true");
						map.put(Constant.WORK_RESPONSE_SYMBOL, "success");
						map.put(Constant.WORK_TIME_SPAN, timeSpan);
					} else {

						Object timeout = data
								.get(Constant.WORK_TIMEOUT_IGNORE);

						// 无超时机制
						if (timeout != null
								&& "true"
										.equals(data
												.get(Constant.WORK_TIMEOUT_IGNORE))) {
							Object ret = handler.doAssignWorks(sct, data);
							map.put(Constant.WORK_DONE_SYMBOL, "true");
							map.put(Constant.WORK_RESPONSE_SYMBOL, ret);
						} else {

							long threshold = Constant.WORK_TIME_SPAN_MAX;
							Object timeoutConfig = data
									.get(Constant.WORK_TIME_CONFIG_THRESHOLD);
							if (timeoutConfig != null
									&& timeoutConfig.toString().matches(
											"[0-9]+")) {
								threshold = Long.parseLong(timeoutConfig
										.toString());
							}

							if (timeSpan <= threshold) {
								Object ret = handler.doAssignWorks(sct, data);
								map.put(Constant.WORK_DONE_SYMBOL, "true");
								map.put(Constant.WORK_RESPONSE_SYMBOL,
										ret);
							} else {
								map.put(Constant.WORK_DONE_SYMBOL,
										"error");
								map.put(Constant.WORK_RESPONSE_SYMBOL,
										"time out ! visit time:" + visitTime
												+ " and deal time:" + nowTime);
							}
						}
						map.put(Constant.WORK_TIME_SPAN, timeSpan);
					}

				} catch (Exception e) {
					map.put(Constant.WORK_DONE_SYMBOL, "error");
					map.put(Constant.WORK_RESPONSE_SYMBOL, "name:"
							+ getId() + ";get exception:" + e.getMessage());
					map.put(Constant.WORK_EXCEPTION, e);
					map.put(Constant.WORK_EXCEPTION_STACK,
							ExceptionTools.getExceptionStack(e));
					map.put(Constant.WORK_TIME_SPAN, timeSpan);
					logger.error("", e);
				} finally {
					Long dealDone = System.currentTimeMillis() - nowTime;
					map.put(Constant.WORK_TIME_COST, dealDone);
				}
				try {
					QueryTools.updateIndexData(getRepositoryClient(),
							searchHit.getIndex(), searchHit.getType(),
							searchHit.getId(), map);
					waitExecutor.execute(new Runnable() {
						@Override
						public void run() {
							waitForUpdate(getRepositoryClient(),
									searchHit.getIndex(), searchHit.getId());
						}
					});

				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					throw new TaskeeperRuntimeException(e);
				} finally {
					if (cacheSet.size() > 1000) {
						cacheSet.poll();
					}

				}

			}

		}
	}

	private boolean waitForUpdate(Client client, String index, String id) {
		for (int i = 0; i < 10; i++) {
			Map<String, Object> source = QueryTools.getIndexAndTypeById(client,
					index, id);
			if (source != null) {
				Object object = source.get(Constant.WORK_DONE_SYMBOL);
				if (!object.equals("false")) {
					return true;
				}
			}
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}

		return false;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setHandler(IServantHandler handler) {
		this.handler = handler;
	}

	protected void init() {

	}

	protected void start() {

	}

	protected void stop() {

	}
}
