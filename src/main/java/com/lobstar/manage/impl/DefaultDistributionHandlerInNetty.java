package com.lobstar.manage.impl;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import com.lobstar.config.Constant;
import com.lobstar.manage.IDistributionHandlerInNetty;

public class DefaultDistributionHandlerInNetty implements IDistributionHandlerInNetty{
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
}
