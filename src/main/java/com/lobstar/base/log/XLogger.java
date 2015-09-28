package com.lobstar.base.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class XLogger{
	

    public static Logger getLogger(Class<?> clazz) {

        return LoggerFactory.getLogger(clazz);
    }
}
