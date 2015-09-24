package com.lobstar.index;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;

import com.lobstar.base.log.XLogger;
import com.lobstar.config.Constant;
import com.lobstar.utils.Utils;

public class QueryTools {
	private static Logger LOG = XLogger.getLogger(QueryTools.class);

	private static Settings settings = ImmutableSettings.settingsBuilder()
			.put("number_of_replicas", 1).build();
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	/**
	 * 生成当天索引
	 * 
	 * @param client
	 *            客户端
	 */
	public static boolean createDailyIndex(Client client) {
		try {
			CreateIndexResponse response = new CreateIndexRequestBuilder(client
					.admin().indices()).setSettings(settings)
					.setIndex(transferTime(new Date().getTime())).execute()
					.actionGet();
			return response.isAcknowledged();

		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return false;
		}
	}

	public static boolean createDailyIndex(Client client,Settings settings) {
		try {
			CreateIndexResponse response = new CreateIndexRequestBuilder(client
					.admin().indices()).setSettings(settings)
					.setIndex(transferTime(new Date().getTime())).execute()
					.actionGet();
			return response.isAcknowledged();

		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return false;
		}
	}
	
	public static boolean backupIndex(Client client, String index) {

		try {
			LOG.info("start to dump index:" + index);

			dumpIndex(client, index, Constant.DUMP_INDEX_NAME,
					Constant.DUMP_TYPE_NAME, 2000);
//			client.prepareDeleteByQuery(index)
//					.setQuery(QueryBuilders.matchAllQuery()).get();

			LOG.info("dump index:" + index + "  finish!");

		} catch (Exception e) {
			LOG.error("dump index" + index + " error!", e);
			return false;
		}
		return true;
	}

	public static void dumpIndex(Client client, String srcIndex,
			String dumpIndex, String dumpType, int tick) {
		SearchResponse response = client.prepareSearch(srcIndex)
				.setQuery(QueryBuilders.matchAllQuery())
				.setSearchType(SearchType.SCAN).setScroll(new TimeValue(60000))
				.setSize(tick).get();

		BulkProcessor processor = BulkProcessor.builder(client, new Listener() {

			@Override
			public void beforeBulk(long executionId, BulkRequest request) {

			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
					BulkResponse response) {

			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
					Throwable failure) {
				LOG.error("dump index error", failure);
			}

		}).setBulkActions(tick).build();

		while (true) {
			SearchHit[] hits = response.getHits().getHits();

			for (SearchHit searchHit : hits) {
				processor.add(new IndexRequest(dumpIndex, dumpType)
						.source(searchHit.getSource()));
			}
			LOG.info("dump index ..." + hits.length);
			response = client.prepareSearchScroll(response.getScrollId())
					.setScroll(new TimeValue(60000)).get();

			if (response.getHits().getHits().length == 0) {
				break;
			}
		}
		processor.close();
	}

	public static boolean createIndex(Client client, String index) {
		try {
			CreateIndexResponse response = new CreateIndexRequestBuilder(client
					.admin().indices()).setIndex(index).setSettings(settings)
					.execute().actionGet();
			LOG.info("create index:" + index);
			return response.isAcknowledged();

		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return false;
		}
	}
	
	public static boolean createIndex(Client client, String index,Settings settings) {
		try {
			CreateIndexResponse response = new CreateIndexRequestBuilder(client
					.admin().indices()).setIndex(index).setSettings(settings)
					.execute().actionGet();
			LOG.info("create index:" + index);
			return response.isAcknowledged();

		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return false;
		}
	}

	public static String getDailyIndex() {
		return transferTime(Utils.getSystemTime());
	}

	public static String getNextDayIndex() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(Utils.getSystemTime());
		calendar.add(Calendar.DATE, 1);
		return transferTime(calendar.getTimeInMillis());
	}

	/**
	 * 
	 * @param client
	 *            客户端
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param data
	 *            插入的索引值
	 */
	public static String insertIndex(Client client, String index, String type,
			Map<String, Object> data) {
		IndexResponse response = new IndexRequestBuilder(client)
				.setIndex(index).setType(type).setSource(data)
				.setRefresh(true)
				.get();
		return response.getId();
	}
	
	public static String insertIndex(Client client, String index, String type,String id,
			Map<String, Object> data) {
		IndexResponse response = new IndexRequestBuilder(client)
				.setIndex(index).setType(type).setId(id).setSource(data)
				.setRefresh(true)
				.get();
		return response.getId();
	}
	

	/**
	 * 
	 * @param client
	 *            客户端
	 * @param index
	 *            索引
	 * @return 是否存在
	 */
	public static boolean isIndexExist(Client client, String index) {
		IndicesExistsResponse response = new IndicesExistsRequestBuilder(client
				.admin().indices(), index).get();
		return response.isExists();
	}

	/**
	 * 根据ID跟新索引
	 * 
	 * @param client
	 *            客户端
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param id
	 *            id
	 * @param data
	 *            替换的值
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static void updateIndexData(Client client, String index,
			String type, String id, Map<String, Object> data) {
		
		client.prepareUpdate(index, type, id).setDoc(data).setRetryOnConflict(5)
		.setRefresh(true)
		.get();

	}

	/**
	 * 替换根据查询语句查出来的索引值
	 * 
	 * @param client
	 *            客户端
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param query
	 *            查询语句
	 * @param data
	 *            替换的值
	 */
	public static void updateIndexData(Client client, String index,
			String type, QueryBuilder query, Map<String, Object> data) {
		updateIndexData(client, index, type, query, data, 500);

	}
	

	public static void updateIndexData(Client client, String index,
			String type, QueryBuilder query, Map<String, Object> data,
			int tickNum) {
		SearchResponse response = new SearchRequestBuilder(client)
				.setIndices(index).setTypes(type).setQuery(query)
				.setSize(tickNum).get();
		SearchHit[] hits = response.getHits().getHits();
		while (hits.length != 0) {
			for (SearchHit searchHit : hits) {
				String id = searchHit.getId();
				String i = searchHit.getIndex();
				String t = searchHit.getType();
				updateIndexData(client, i, t, id, data);
			}

			response = new SearchRequestBuilder(client).setIndices(index)
					.setTypes(type).setQuery(query).setSize(tickNum).get();

			hits = response.getHits().getHits();
		}
	}

	public static SearchResponse searchIndexAndType(Client client,
			String index, String type, QueryBuilder query) {

		return searchIndexAndType(client, index, type, query, 500);
	}

	public static SearchResponse searchIndexAndType(Client client,
			String index, String type, QueryBuilder query, int tickNum) {

		SearchRequestBuilder requestBuilder = new SearchRequestBuilder(client)
				.setIndices(index).setTypes(type);
		if (query != null) {
			requestBuilder.setQuery(query);
		}
		requestBuilder.setSize(tickNum);
		SearchResponse response = requestBuilder.get();
		return response;
	}

	public static Map<String, Object> getIndexAndTypeById(Client client,
			String index, String id) {
		GetResponse response = new GetRequestBuilder(client).setIndex(index)
				.setId(id).get();

		return response.getSource();
	}
	public static Map<String, Object> getIndexAndTypeById(Client client,
			String index,String type, String id) {
		GetResponse response = new GetRequestBuilder(client).setIndex(index).setType(type)
				.setId(id).get();

		return response.getSource();
	}

	public static SearchResponse searchIndexAndType(Client client,
			String index, String type) {
		return searchIndexAndType(client, index, type, null, 500);
	}

	public static SearchResponse searchIndexAndType(Client client,
			String index, String type, int tickNum) {
		return searchIndexAndType(client, index, type, null, tickNum);
	}

	public static void updateIndexData(Client client, String index,
			QueryBuilder query, Map<String, Object> data) {
		updateIndexData(client, index, query, data, 500);

	}

	public static void updateIndexData(Client client, String index,
			QueryBuilder query, Map<String, Object> data, int tickNum) {
		SearchResponse response = new SearchRequestBuilder(client)
				.setIndices(index).setQuery(query).setSize(tickNum).get();

		SearchHit[] hits = response.getHits().getHits();

		while (hits.length > 0) {
			for (SearchHit searchHit : hits) {
				String id = searchHit.getId();
				String i = searchHit.getIndex();
				String t = searchHit.getType();
				Map<String, Object> source = searchHit.getSource();
				source.putAll(data);
				updateIndexData(client, i, t, id, source);
			}

			response = new SearchRequestBuilder(client).setIndices(index)
					.setQuery(query).setSize(tickNum).get();

			hits = response.getHits().getHits();
		}

	}

	public static void moveIndexData(Client client, String index,
			String sourceType, String targetType, QueryBuilder query) {
		moveIndexData(client, index, sourceType, targetType, query, null);
	}

	public static void moveIndexData(Client client, String index,
			String sourceType, String targetType, String id,
			Map<String, Object> extraData) {
		GetResponse response = new GetRequestBuilder(client).setIndex(index)
				.setType(sourceType).setId(id).get();
		BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);
		Map<String, Object> source = response.getSource();
		if (extraData != null && source != null) {
			source.putAll(extraData);
		} else if (extraData != null) {
			source = new HashMap<String, Object>(extraData);
		}
		IndexRequestBuilder requestBuilder = new IndexRequestBuilder(client)
				.setIndex(index).setType(targetType).setId(response.getId())
				.setRefresh(true)
				.setSource(source);

		DeleteRequestBuilder deleteRequestBuilder = new DeleteRequestBuilder(
				client).setIndex(index).setType(sourceType)
				.setRefresh(true)
				.setId(response.getId());

		bulkRequestBuilder.add(requestBuilder).add(deleteRequestBuilder)
				.execute();
	}
	
	public static void moveIndexData(Client client, String index,
			String sourceType, String[] targetTypes, String id,
			Map<String, Object> extraData) {
		GetResponse response = new GetRequestBuilder(client).setIndex(index)
				.setType(sourceType).setId(id).get();
		BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);
		Map<String, Object> source = response.getSource();
		if (extraData != null && source != null) {
			source.putAll(extraData);
		} else if (extraData != null) {
			source = new HashMap<String, Object>(extraData);
		}
		
		for(String type : targetTypes) {
			IndexRequestBuilder requestBuilder = new IndexRequestBuilder(client)
			.setIndex(index).setType(type).setId(response.getId())
			.setRefresh(true)
			.setSource(source);	
			bulkRequestBuilder.add(requestBuilder);
		}

		DeleteRequestBuilder deleteRequestBuilder = new DeleteRequestBuilder(
				client).setIndex(index).setType(sourceType)
				.setRefresh(true)
				.setId(response.getId());

		bulkRequestBuilder.add(deleteRequestBuilder)
				.execute();
	}

	public static void moveIndexData(Client client, String index,
			String sourceType, String targetType, String id) {
		moveIndexData(client, index, sourceType, targetType, id, null);
	}

	public static void moveIndexData(Client client, String index,
			String sourceType, String targetType, QueryBuilder query,
			Map<String, Object> extraData) {
		moveIndexData(client, index, sourceType, targetType, query, extraData,
				500);
	}

	public static void moveIndexData(Client client, String index,
			String sourceType, String targetType, QueryBuilder query,
			Map<String, Object> extraData, int tickNum) {
		SearchResponse response = new SearchRequestBuilder(client)
				.setIndices(index).setTypes(sourceType).setQuery(query)
				.setSize(tickNum).get();

		SearchHit[] hits = response.getHits().getHits();
		while (hits.length > 0) {
			BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(
					client);
			for (SearchHit searchHit : hits) {
				Map<String, Object> source = searchHit.getSource();
				if (extraData != null) {
					source.putAll(extraData);
				}
				IndexRequestBuilder requestBuilder = new IndexRequestBuilder(
						client).setIndex(index).setType(targetType)
						.setId(searchHit.getId())
						.setRefresh(true)
						.setSource(source);
				bulkRequestBuilder.add(requestBuilder);
				DeleteRequestBuilder deleteRequestBuilder = new DeleteRequestBuilder(
						client).setIndex(index).setType(sourceType)
						.setRefresh(true)
						.setId(searchHit.getId());
				bulkRequestBuilder.add(deleteRequestBuilder);
			}
			bulkRequestBuilder.execute();

			response = new SearchRequestBuilder(client).setIndices(index)
					.setTypes(sourceType).setQuery(query).setSize(tickNum)
					.get();

			hits = response.getHits().getHits();
		}

	}


	public static String transferTime(Long time) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(time);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return String.valueOf(calendar.getTimeInMillis());
	}
	
	public static BulkProcessor buildProcessor(Client client,int tick) {
		BulkProcessor processor = BulkProcessor.builder(client, new Listener() {

			@Override
			public void beforeBulk(long executionId, BulkRequest request) {

			}
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					BulkResponse response) {

			}
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					Throwable failure) {
				LOG.error(executionId+":"+failure);
			}

		}).setBulkActions(tick).build();
		return processor;
	}

}
