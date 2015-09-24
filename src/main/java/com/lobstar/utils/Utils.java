package com.lobstar.utils;


public class Utils {
    public static long getSystemTime() {
        return System.currentTimeMillis();
    }
    
    public static String contact(Object...strs) {
    	StringBuffer buffer = new StringBuffer();
    	if(strs != null) {
    		for (Object s : strs) {
    			buffer.append(s);
    		}
    	}
    	return buffer.toString();    		
    }
}
