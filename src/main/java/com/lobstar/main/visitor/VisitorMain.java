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
        for (int i = 0; i <1; i++) {
        	pool.execute(new VisitorMain().new Task(i));
//        	if(i%100 == 0) {
//        		Thread.sleep(200);
//        	}
        }
        System.out.println("done");
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
            data.put("_domain", "_manage");
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
				System.out.println(visitor.getReturnValue());
				System.out.println(visitor.getReturnError());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            visitor.close();
    	}
    }

}
