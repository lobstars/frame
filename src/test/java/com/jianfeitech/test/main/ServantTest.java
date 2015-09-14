package com.jianfeitech.test.main;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import com.lobstar.base.role.Servant;
import com.lobstar.config.BuildConfiguration;
import com.lobstar.context.ServantContext;
import com.lobstar.manage.IServantHandler;

public class ServantTest {
	public static void main(String[] args) throws Exception{
		Servant baseServant = null;
        try {
        	Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true)
                    .put("cluster.name", "x-factory").build();
        	
        	
            baseServant = new Servant("test","127.0.0.1:12888",settings,"127.0.0.1",9300);
            baseServant.setDomain("test");
            baseServant.setHandler(new IServantHandler() {
                @Override
                public Object doAssignWorks(ServantContext sc,Map<String, Object> source) throws Exception{
                    	Object num1 = source.get("num1");
                    	Object num2 = source.get("num2");
                    	long ret=0;
                    	if(num1!= null && num2 != null) {
                    		ret = Long.parseLong(num1.toString())+Long.parseLong(num2.toString());
                    		System.out.println("get: "+ret);
                    		Thread.sleep(ret);
                    	}
                    return ret;
                }
            });
            baseServant.join();
        } catch (Exception e) {
            if (baseServant != null) {
                baseServant.close();
            }
        }
	}
}
