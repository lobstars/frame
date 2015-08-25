package com.lobstar.manage;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public interface IDistributionHandlerInNetty {
    public String distribution(Map<String, Object> data, CopyOnWriteArrayList<String> workerSet,
            Map<String, CopyOnWriteArrayList<String>> domainWorkerMap);
}
