package com.lobstar.base.role.master;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ServantGroup {

	private static CopyOnWriteArrayList<String> workerSet = new CopyOnWriteArrayList<String>();

    private static Map<String, CopyOnWriteArrayList<String>> domainWorkerMap = new ConcurrentHashMap<String, CopyOnWriteArrayList<String>>();

	public static CopyOnWriteArrayList<String> getWorkerSet() {
		return workerSet;
	}

	public static void setWorkerSet(CopyOnWriteArrayList<String> workerSet) {
		ServantGroup.workerSet = workerSet;
	}

	public static Map<String, CopyOnWriteArrayList<String>> getDomainWorkerMap() {
		return domainWorkerMap;
	}

	public static void setDomainWorkerMap(
			Map<String, CopyOnWriteArrayList<String>> domainWorkerMap) {
		ServantGroup.domainWorkerMap = domainWorkerMap;
	}
    
    
}
