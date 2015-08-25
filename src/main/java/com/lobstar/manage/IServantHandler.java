package com.lobstar.manage;

import java.util.Map;

import com.lobstar.context.ServantContext;

public interface IServantHandler {

    public Object doAssignWorks(ServantContext sc,Map<String, Object> source) throws Exception;
}
