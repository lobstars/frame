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

import com.lobstar.base.gateway.NettyGateWay;
import com.lobstar.base.gateway.ParkGateWay;
import com.lobstar.base.role.Mission;
import com.lobstar.manage.IWorkerListener;

public class VisitorMain {

    public static void main(String[] args) throws Exception {
    	
    	ExecutorService pool = Executors.newFixedThreadPool(200);
        

        //        while (true) {
        for (int i = 0; i <10000; i++) {
        	if(i%20 == 0) {
        		Thread.sleep(20);
        	}
        	if(i%35 == 0) {
        		Thread.sleep(50);
        	}
        	Map<String, Object> data = new HashMap<String, Object>();
            List<String> param = new ArrayList<String>();
            param.add("p1");
            param.add("p3");
            data.put("symbol", "0053");
            data.put("code", "1111");
            data.put("action","send");
            data.put("_domain", "_status");
            data.put("temp_params", param);
            Mission visitor = new Mission("127.0.0.1", 10888);
            visitor.setData(data);
            //System.out.println("send "+index);
            final long timeMillis1 = System.currentTimeMillis();
            try {
            	visitor.submit();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            visitor.getResponse(100, 500, new IWorkerListener<Mission>() {
				@Override
				public void responseVisitor(Mission visitor, Object ret) {
					Map<String,Object> object = (Map<String, Object>) ret;
					System.out.println(object.get("_wait_time"));
				}
			});
        }
        Thread.sleep(5000);
        pool.shutdown();
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
            Mission visitor = new Mission("127.0.0.1", 10888);
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
