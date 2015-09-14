package com.lobstar.config;

public class Constant {


    //baseServant 轮询任务时间间隔
    public final static int SERVANT_POLL_INTERVAL = 100;

    //处理任务线程数
    public final static int SERVANT_HANDLE_WORK_THREAD_NUM = 200;

    public final static int SERVANT_TASK_QUEUE_SIZE = 3000;

    public final static int TICKET_PORT = 10888;

    public final static String LOCAL_ADDRESS = "127.0.0.1";

    public final static String GLOBAL_CHARSET = "utf-8";

    public final static String WORK_DONE_SYMBOL = "_done";

    public final static String WORK_ASSIGN_SYMBOL = "_assign";

    public final static String WORK_UNDO_SYMBOL = "_undo";

    public final static String WORK_NOT_ASSIGN_TYPE = "_no_assign";

    public final static String WORK_RESPONSE_SYMBOL = "_return";
    
    public final static String WORK_RESPONSE_ASYNC_SYMBOL = "_async_return";
    
    public final static String WORK_RESPSONSE_ASYNC_TASK_ID = "_async_param_id";
    
    public final static String WORK_RESPSONSE_ASYNC_TASK_INDEX = "_async_param_index";
    
    public final static String WORK_RESPONSE_IGNORE = "_return_ignore";

    //标识作用
    public final static String WORK_DOMAIN_SYMBOL = "_domain";
    
    public final static String WORK_DOMAIN_BROADCAST = "_broadcast_";
    //标识地域
    public final static String WORK_REGION_SYMBOL = "_region";
    
    public final static String WORK_TIMEOUT_IGNORE = "_timeout_ignore";
    
    public final static String WORK_TIME_SPAN = "_wait_time";
    
    public final static String WORK_TIME_COST = "_deal_time";
    
    public final static String WORK_TIME_CONFIG_THRESHOLD = "_timeout_threshold";
    
    public final static long WORK_TIME_SPAN_MAX = 18000000L;

    public final static String VISITOR_TIME_SYMBOL = "_visit_time";

    public final static String VISITOR_TIMEZONE_SYMBOL = "_visit_timezone";

    public final static String WORK_EXCEPTION = "_exception";

    public final static String WORK_EXCEPTION_STACK = "_exception_stack";
    
    public final static String DUMP_INDEX_NAME = "dump_index";
    
    public final static String DUMP_TYPE_NAME = "dump_type";
    
    public static final int MASTER_POLL_INTERVAL = 200;
    
    public static final int MASTER_TICKET_WINDOW_THREAD_SIZE = 300;
}
