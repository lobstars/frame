package com.lobstar.base.exception;

public class TaskeeperRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 2583306547644346332L;

    public TaskeeperRuntimeException(String msg) {
        super(msg);
    }

    public TaskeeperRuntimeException(Exception e) {
        super(e.getMessage(), e);
    }

    public TaskeeperRuntimeException(String msg, Exception e) {
        super(msg, e);
    }
}
