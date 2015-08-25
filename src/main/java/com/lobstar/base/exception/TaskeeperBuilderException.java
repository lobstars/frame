package com.lobstar.base.exception;

public class TaskeeperBuilderException extends RuntimeException {

    private static final long serialVersionUID = -8002873699685251868L;

    public TaskeeperBuilderException(String msg) {
        super(msg);
    }

    public TaskeeperBuilderException(Exception e) {
        super(e.getMessage(), e);
    }

    public TaskeeperBuilderException(String msg, Exception e) {
        super(msg, e);
    }
}
