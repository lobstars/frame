package com.lobstar.manage;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticsearch.search.SearchHit;

public interface IDistributionHandler {

    public String distribution(Map<String, Object> source, CopyOnWriteArrayList<String> workerSet,
            Map<String, CopyOnWriteArrayList<String>> domainWorkerMap);
}
