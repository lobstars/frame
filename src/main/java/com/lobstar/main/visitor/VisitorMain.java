package com.lobstar.main.visitor;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.index.SegmentInfos.FindSegmentsFile;
import org.codehaus.jackson.map.ObjectMapper;

import com.lobstar.base.gateway.NettyGateWay;
import com.lobstar.base.gateway.ParkGateWay;
import com.lobstar.base.role.Mission;
import com.lobstar.base.role.MissionReport;
import com.lobstar.manage.IWorkerListener;

public class VisitorMain {

    public static void main(String[] args) throws Exception {
            List<String> param = new ArrayList<String>();
            param.add("p1");
            param.add("p3");
           
            try {
            	 MissionReport report = new Mission("115.28.9.13", 10888)
         		.addParam("symbol", "test")
         		.addParam("code", "1111")
         		.addParam("action", "send")
         		.addParam("temp_params", param)
         		.addParam("phone", "111")
         		.submit().reportGet();
            	 if(report.isError()) {
            		 System.out.println(report.getException());
            	 }else {
            		 System.out.println(report.getResult());            		 
            	 }
            } catch (Exception e) {
                e.printStackTrace();
            }
    }

    class Task implements Runnable {
    	int index;
    	
    	public Task(int index) {
    		this.index = index;
    	}
    	@Override
    	public void run() {

            Map<String, Object> data = new HashMap<String, Object>();
            List<String> param = new ArrayList<String>();
            param.add("p1");
            param.add("p3");
            data.put("symbol", "0053");
            data.put("phone", "1020030"+index);
            data.put("code", "1111");
            data.put("action","send");
            data.put("_domain", "_status");
            data.put("temp_params", param);
            Mission visitor = new Mission("115.28.9.13", 10888);
            visitor.setData(data);
            //System.out.println("send "+index);
            final long timeMillis1 = System.currentTimeMillis();
            try {
            	visitor.submit();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        	try {
        		Map<String,Object> object = (Map<String, Object>) visitor.getResponse(500, 200);
				System.out.println(object.get("_wait_time"));
			} catch (Exception e) {
				e.printStackTrace();
			}
            visitor.close();
    	}
    }

}
