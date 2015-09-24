package com.lobstar.base.service.status;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lobstar.base.role.Servant;
import com.lobstar.base.service.manage.ManagerServant;
import com.lobstar.config.BuildConfiguration;
import com.lobstar.config.Constant;
import com.lobstar.context.ServantContext;
import com.lobstar.manage.IServantHandler;

public class StatusServant {
	private static final Logger logger = LoggerFactory.getLogger(ManagerServant.class);
	
	private static StatusServant ss;
	
	private static boolean active = false;
	private final String managerServantName = Constant.SYSTEM_SERVANT_STATUS;
	private final String managerDomain = "_status";
	private final String actionName = "action";
	private static Servant statusServant;
	
	private StatusServant(){
		try {
			statusServant = new Servant(managerServantName, new BuildConfiguration().buildConfig());
			statusServant.setDomain(managerDomain);
			statusServant.setHandler(new IServantHandler() {
				@Override
				public Map<String,Object> doAssignWorks(ServantContext sc, Map<String, Object> source)
						throws Exception {
					Object action = source.get(actionName);
					System.out.println(action);
					return null;
				}
			});
		} catch (IOException e) {
			logger.error("",e);
		}
	}
	
	private static void activeServant() {
		statusServant.join();
	}
	
	
	public synchronized static void startService() {
		if(ss == null) {
			ss = new StatusServant();
		}
		if(!active) {
			activeServant();
			active = true;
		}
	}
}
