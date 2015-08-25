package com.lobstar.index;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;

import com.lobstar.base.log.XLogger;

public class FilterTools {

    private static Logger LOG = XLogger.getLogger(FilterTools.class);

    /**
     * 生成当天索引
     * @param client 客户端
     */
    public static boolean createDailyIndex(Client client) {
        try {
            CreateIndexResponse response = new CreateIndexRequestBuilder(client.admin().indices())
                    .setIndex(transferTime(new Date().getTime())).execute().actionGet();
            return response.isAcknowledged();

        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            return false;
        }
    }

    public static String getDailyIndex() {
        return transferTime(new Date().getTime());
    }

    /**
     * 
     * @param client 客户端
     * @param index 索引
     * @param type 类型
     * @param data 插入的索引值
     */
    public static void insertIndex(Client client, String index, String type, Map<String, Object> data) {
        new IndexRequestBuilder(client).setIndex(index).setType(type).setSource(data).execute();
    }

    /**
     * 
     * @param client 客户端
     * @param index 索引
     * @return 是否存在
     */
    public static boolean isIndexExist(Client client, String index) {
        IndicesExistsResponse response = new IndicesExistsRequestBuilder(client.admin().indices(), index).get();
        return response.isExists();
    }

    /**
     * 根据ID跟新索引
     * @param client 客户端
     * @param index 索引
     * @param type 类型
     * @param id id
     * @param data 替换的值，类似于先删除再添加
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public static void updateIndexData(Client client, String index, String type, String id, Map<String, Object> data) {
        Map<String, Object> source = new GetRequestBuilder(client).setIndex(index).setType(type).setId(id).get()
                .getSource();
        if (source != null) {
            source.putAll(data);
        }
        new IndexRequestBuilder(client).setSource(source).setIndex(index).setType(type).setId(id).execute();
    }

    public static void bulkUpdateIndexData(Client client, String index, String type, List<String> ids,
            Map<String, Object> data) {
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        for (String id : ids) {
            Map<String, Object> source = new GetRequestBuilder(client).setIndex(index).setType(type).setId(id).get()
                    .getSource();
            if (source != null) {
                source.putAll(data);
            }
            IndexRequestBuilder requestBuilder = new IndexRequestBuilder(client).setSource(source).setIndex(index)
                    .setType(type).setId(id);
            bulkRequestBuilder.add(requestBuilder);
        }
        bulkRequestBuilder.execute();

    }

    /**
     * 替换根据查询语句查出来的索引值
     * @param client 客户端
     * @param index 索引
     * @param type 类型
     * @param query 查询语句
     * @param data 替换的值
     */
    public static void updateIndexData(Client client, String index, String type, FilterBuilder query,
            Map<String, Object> data) {
        updateIndexData(client, index, type, query, data, 500);

    }

    public static void updateIndexData(Client client, String index, String type, FilterBuilder filter,
            Map<String, Object> data, int tickNum) {
        SearchResponse response = new SearchRequestBuilder(client).setIndices(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).setSize(tickNum).get();

        SearchHit[] hits = response.getHits().getHits();
        while (hits.length != 0) {
            List<String> ids = new ArrayList<String>();
            for (SearchHit searchHit : hits) {
                ids.add(searchHit.getId());

            }
            bulkUpdateIndexData(client, index, type, ids, data);
            response = new SearchRequestBuilder(client).setIndices(index).setTypes(type)
                    .setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).setSize(tickNum).get();

            hits = response.getHits().getHits();
        }
    }

    public static SearchResponse searchIndexAndType(Client client, String index, String type, FilterBuilder filter) {

        return searchIndexAndType(client, index, type, filter, 500);
    }

    public static SearchResponse searchIndexAndType(Client client, String index, String type, FilterBuilder filter,
            int tickNum) {

        SearchRequestBuilder requestBuilder = new SearchRequestBuilder(client).setIndices(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery());
        if (filter != null) {
            requestBuilder.setPostFilter(filter);
        }
        requestBuilder.setSize(tickNum);
        SearchResponse response = requestBuilder.get();
        return response;
    }

    public static SearchResponse searchIndexAndType(Client client, String index, String type, int tickNum) {
        return searchIndexAndType(client, index, type, null, tickNum);
    }

    public static SearchResponse searchIndexAndType(Client client, String index, String type) {
        return searchIndexAndType(client, index, type, null, 500);
    }

    public static void updateIndexData(Client client, String index, FilterBuilder filter, Map<String, Object> data) {
        updateIndexData(client, index, filter, data, 500);

    }

    public static void updateIndexData(Client client, String index, FilterBuilder filter, Map<String, Object> data,
            int tickNum) {
        SearchResponse response = new SearchRequestBuilder(client).setIndices(index)
                .setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).setSize(tickNum).get();

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

            response = new SearchRequestBuilder(client).setIndices(index).setQuery(QueryBuilders.matchAllQuery())
                    .setPostFilter(filter).setSize(tickNum).get();

            hits = response.getHits().getHits();
        }

    }

    public static void moveIndexData(Client client, String index, String sourceType, String targetType,
            FilterBuilder filter) {
        moveIndexData(client, index, sourceType, targetType, filter, null);
    }

    public static void moveIndexData(Client client, String index, String sourceType, String targetType, String id,
            Map<String, Object> extraData) {
        GetResponse response = new GetRequestBuilder(client).setIndex(index).setType(sourceType).setId(id).get();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);
        Map<String, Object> source = response.getSource();
        if (extraData != null && source != null) {
            source.putAll(extraData);
        } else if (extraData != null) {
            source = new HashMap<String, Object>(extraData);
        }
        IndexRequestBuilder requestBuilder = new IndexRequestBuilder(client).setIndex(index).setType(targetType)
                .setId(response.getId()).setSource(source);

        DeleteRequestBuilder deleteRequestBuilder = new DeleteRequestBuilder(client).setIndex(index)
                .setType(sourceType).setId(response.getId());

        bulkRequestBuilder.add(requestBuilder).add(deleteRequestBuilder).execute();
    }

    public static void moveIndexData(Client client, String index, String sourceType, String targetType, String id) {
        moveIndexData(client, index, sourceType, targetType, id, null);
    }

    public static void moveIndexData(Client client, String index, String sourceType, String targetType,
            FilterBuilder filter, Map<String, Object> extraData) {
        moveIndexData(client, index, sourceType, targetType, filter, extraData, 500);
    }

    public static void moveIndexData(Client client, String index, String sourceType, String targetType,
            FilterBuilder filter, Map<String, Object> extraData, int tickNum) {
        SearchResponse response = new SearchRequestBuilder(client).setIndices(index).setTypes(sourceType)
                .setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).setSize(tickNum).get();

        SearchHit[] hits = response.getHits().getHits();
        while (hits.length > 0) {
            BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);
            for (SearchHit searchHit : hits) {
                Map<String, Object> source = searchHit.getSource();
                if (extraData != null) {
                    source.putAll(extraData);
                }
                IndexRequestBuilder requestBuilder = new IndexRequestBuilder(client).setIndex(index)
                        .setType(targetType).setId(searchHit.getId()).setSource(source);
                bulkRequestBuilder.add(requestBuilder);
                DeleteRequestBuilder deleteRequestBuilder = new DeleteRequestBuilder(client).setIndex(index)
                        .setType(sourceType).setId(searchHit.getId());
                bulkRequestBuilder.add(deleteRequestBuilder);
            }
            bulkRequestBuilder.execute();

            response = new SearchRequestBuilder(client).setIndices(index).setTypes(sourceType)
                    .setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).setSize(tickNum).get();

            hits = response.getHits().getHits();
        }

    }

    public static String transferTime(Long time) {
        return String.valueOf(time / 86400000 * 86400000);
    }

}
