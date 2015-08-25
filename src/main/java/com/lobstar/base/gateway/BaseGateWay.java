package com.lobstar.base.gateway;

import com.lobstar.manage.IWorkerListener;

public abstract class BaseGateWay<T> {

    public BaseGateWay() {
        
    }

    public abstract void submit(T visitor) throws Exception;

    public abstract void getResponse(T visitor, IWorkerListener<T> listener);

    public Object getResponse(T visitor) throws Exception {
        return getResponse(visitor, 10, 500);
    }

    public abstract Object getResponse(T visitor, int tryNum, int sleepTime) throws Exception;

    
}
