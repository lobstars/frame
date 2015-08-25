package com.lobstar.manage;

public interface IWorkerListener<T> {
    public void responseVisitor(T visitor, Object ret);
}
