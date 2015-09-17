package com.lobstar.main.servant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;

import com.lobstar.base.log.XLogger;
import com.lobstar.base.role.Servant;
import com.lobstar.config.BuildConfiguration;
import com.lobstar.context.ServantContext;
import com.lobstar.index.QueryTools;
import com.lobstar.manage.IServantHandler;

public class ServantMain {

    public static void main(String[] args) throws IOException {
        final Logger logger = XLogger.getLogger(ServantMain.class);

        String name;
        if (args.length < 1) {
            name = UUID.randomUUID().toString();
        } else {
            name = args[0];
        }
        String domain = "default";
        if (args.length >= 2) {
            domain = args[1];
        }
        name = "test";
        Servant baseServant = null;
        try {
            baseServant = new Servant(name, new BuildConfiguration().buildConfig());
            baseServant.setDomain(domain);
            baseServant.setHandler(new IServantHandler() {
                @Override
                public Object doAssignWorks(final ServantContext sc,Map<String, Object> source) {
                    new Thread(){
                    	public void run() {	
                    		try {
                    			Object ret = "final resp";
                    			System.out.println(">>>>>>>>>>>>>>>>>>>in");
								Thread.sleep(20000);
								System.out.println(">>>>>>>>>>>>>>>>>>>done");
								sc.updateResponse(ret);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
                    	};
                    }.start();
                    return "success";
                }
            });
           
            baseServant.join();
            System.out.println("---------->");
        } catch (Exception e) {
        	logger.error("",e);
            if (baseServant != null) {
                baseServant.close();
            }
        }

    }
}
