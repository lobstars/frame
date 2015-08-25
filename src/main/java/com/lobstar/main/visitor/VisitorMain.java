package com.lobstar.main.visitor;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.lobstar.base.gateway.NettyGateWay;
import com.lobstar.base.gateway.ParkGateWay;
import com.lobstar.base.role.Mission;
import com.lobstar.manage.IWorkerListener;

public class VisitorMain {

    public static void main(String[] args) throws Exception {

        final NettyGateWay gateWay = new NettyGateWay();
        long total = 0;

        //        while (true) {
        for (int i = 0; i < 1; i++) {

            Map<String, Object> data = new HashMap<String, Object>();

            data.put("pwd", "sclt");
            data.put("_broadcast_", "true");
            data.put("action","_reload");
            Mission visitor = new Mission("127.0.0.1", 10888);
            visitor.setData(data);
            final long timeMillis1 = System.currentTimeMillis();
            try {
                gateWay.submit(visitor);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeMillis2 = System.currentTimeMillis();
            Object response = gateWay.getResponse(visitor, 40, 1000);
            long timeMillis3 = System.currentTimeMillis();

            long t = timeMillis2 - timeMillis1;
            System.out.println(response + ":" + (t));
            total += t;
            System.out.println("--"+(timeMillis3-timeMillis2));
            visitor.close();
        }
        System.out.println("--------------------------" + total + "----------------------------------");
        System.out.println(total / 20);
        total = 0;
        Thread.sleep(500);
    }

    //            gateWay.getResponse(visitor, new IWorkerListener() {
    //
    //                @Override
    //                public void responseVisitor(Object visitor, Object ret) {
    //                }
    //            });

    //    }


}
