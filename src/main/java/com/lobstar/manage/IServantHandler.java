package com.lobstar.manage;

import java.util.Map;

import com.lobstar.context.ServantContext;

public interface IServantHandler {

    public Map<String,Object> doAssignWorks(ServantContext sc,Map<String, Object> source) throws Exception;

//    public void doAssignWorksSyn(ServantContext sc,Map<String, Object> source,IServantHandler.CallBack callback) throws Exception;
//
//    interface CallBack {
//    	public void callback();
//    }

}
