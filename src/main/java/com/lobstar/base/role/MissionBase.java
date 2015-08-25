package com.lobstar.base.role;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.slf4j.Logger;

import com.lobstar.base.log.XLogger;
import com.lobstar.config.Constant;
import com.lobstar.index.QueryTools;
import com.lobstar.manage.IWorkerListener;

public class MissionBase {

    private static final Logger LOG = XLogger.getLogger(MissionBase.class);

    private String isDone = "false";
    private String isAssign = "false";
    private Map<String, Object> data;

    private String id;
    private String index;

    public void submit(Client client) throws InterruptedException {
        if (data == null) {
            data = new HashMap<String, Object>();
        }
        data.put(Constant.WORK_ASSIGN_SYMBOL, isAssign);
        data.put(Constant.WORK_DONE_SYMBOL, isDone);
        data.put(Constant.VISITOR_TIMEZONE_SYMBOL, Calendar.getInstance().getTimeZone().getID());
        this.index = QueryTools.getDailyIndex();
        this.id = QueryTools.insertIndex(client, index, Constant.WORK_NOT_ASSIGN_TYPE, data);
    }

    public Object getResponse(Client client, int tryNum, int sleeptime) {
        for (int i = 0; i < tryNum; i++) {
            Map<String, Object> source = QueryTools.getIndexAndTypeById(client, index, id);
            if (source != null) {
                Object ret = source.get(Constant.WORK_RESPONSE_SYMBOL);
                if (ret != null) {
                    return ret;
                }
            }
            try {
                Thread.sleep(sleeptime);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(),e);
            }
        }
        return null;
    }

    public void getResponse(Client client, int tryNum, int sleeptime, IWorkerListener listener) {
        new SeekResponse(client, tryNum, sleeptime, listener, this).start();

    }

    private class SeekResponse extends Thread {

        private Client client;
        private int tryNum;
        private int sleeptime;
        private IWorkerListener listener;
        private MissionBase visitor;

        public SeekResponse(Client client, int tryNum, int sleeptime, IWorkerListener listener, MissionBase visitor) {
            this.client = client;
            this.tryNum = tryNum;
            this.sleeptime = sleeptime;
            this.listener = listener;
            this.visitor = visitor;
        }

        @Override
        public void run() {
            Object ret = getResponse(client, tryNum, sleeptime);
            listener.responseVisitor(visitor, ret);
        }
    }
}
