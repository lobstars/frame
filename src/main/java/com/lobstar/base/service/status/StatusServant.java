package com.lobstar.base.service.status;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.antlr.grammar.v3.ANTLRParser.action_return;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lobstar.base.role.Servant;
import com.lobstar.base.service.manage.ManagerServant;
import com.lobstar.config.Builder;
import com.lobstar.config.Constant;
import com.lobstar.context.ServantContext;
import com.lobstar.index.QueryTools;
import com.lobstar.manage.IServantHandler;

public class StatusServant {
	private static final Logger logger = LoggerFactory.getLogger(ManagerServant.class);
	
	private static StatusServant ss;
	
	private static boolean active = false;
	private final String statusServantName = Constant.SYSTEM_SERVANT_STATUS;
	private final String statusDomain = "_status";
	private final String actionName = "_action";
	private static Servant statusServant;
	
	private StatusServant(){
		try {
			statusServant = new Servant(statusServantName, new Builder().buildConfig());
			statusServant.setDomain(statusDomain);
			statusServant.setHandler(new IServantHandler() {
				@Override
				public Map<String,Object> doAssignWorks(final ServantContext sc, Map<String, Object> source)
						throws Exception {
					Object action = source.get(actionName);
					if(action == null) {
						throw new Exception("no action");
					}
					System.out.println(action);
					Map<String,Object> ret = new HashMap<String, Object>();
					switch (action.toString()) {
						case "getAsyncResponse" :
							Object index = source.get("index");
							Object type = source.get("type");
							Object id = source.get("id");
							if(index == null || type == null || id == null) {
								throw new Exception("id or type or index is null");
							}
							ret = QueryTools.getIndexAndTypeById(statusServant.getRepositoryClient(), index.toString(), type.toString(), id.toString());
							break;

						default :
							Thread.sleep(5000);
							break;
					}
					return ret;
				}
			});
		} catch (IOException e) {
			logger.error("",e);
		}
	}
	
	private static void activeServant() {
		statusServant.work();
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
