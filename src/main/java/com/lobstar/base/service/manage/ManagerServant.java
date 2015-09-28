package com.lobstar.base.service.manage;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lobstar.base.role.Servant;
import com.lobstar.config.Builder;
import com.lobstar.config.Constant;
import com.lobstar.context.ServantContext;
import com.lobstar.manage.IServantHandler;

public class ManagerServant {
	private static final Logger logger = LoggerFactory.getLogger(ManagerServant.class);
	
	private static ManagerServant ms;
	
	private static boolean active = false;
	private final String managerServantName = Constant.SYSTEM_SERVANT_MANAGER;
	private final String managerDomain = "_manage";
	
	private Servant managerServant;
	
	private ManagerServant(){
		try {
			managerServant = new Servant(managerServantName, new Builder().buildConfig());
			managerServant.setDomain(managerDomain);
			managerServant.setHandler(new IServantHandler() {
				@Override
				public Map<String,Object> doAssignWorks(ServantContext sc, Map<String, Object> source)
						throws Exception {
					return null;
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void activeServant() {
		
	}
	
	
	public synchronized static void startService() {
		if(ms == null) {
			ms = new ManagerServant();
		}
		if(!active) {
			activeServant();
		}
	}
}
