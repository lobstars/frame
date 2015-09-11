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
    	
    	ExecutorService pool = Executors.newFixedThreadPool(1);
        

        //        while (true) {
        for (int i = 0; i <1; i++) {
        	pool.execute(new VisitorMain().new Task(i));
        	if(i%20 == 0) {
        		Thread.sleep(200);
        	}
        }
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
            data.put("symbol", "aa");
            data.put("phone", "18510867941");
            data.put("code", "kkk");
            data.put("signature", "汉字");
            data.put("action","send");
            data.put("temp_params", param);
//            data.put("_return_ignore", "true");
//            data.put("action", "update_config");
//            data.put("_broadcast_", "true");
            Mission visitor = new Mission("115.28.9.13", 10888);
            visitor.setData(data);
            System.out.println("send "+index);
            final long timeMillis1 = System.currentTimeMillis();
            try {
            	visitor.submit();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        	try {
				System.out.println(visitor.getReturnValue());
				System.out.println(visitor.getReturnError());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
//            long timeMillis2 = System.currentTimeMillis();
            //Object response = visitor.getResponse(1000, 10);
//            long timeMillis3 = System.currentTimeMillis();
//
//            long t = timeMillis2 - timeMillis1;
//            System.out.println(index+"--"+(timeMillis3-timeMillis2));
            visitor.close();
    	}
    }

}
