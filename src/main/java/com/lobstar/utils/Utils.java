package com.lobstar.utils;


public class Utils {
    public static long getSystemTime() {
        return System.currentTimeMillis();
    }
    
    public static String contact(String...strs) {
    	StringBuffer buffer = new StringBuffer();
    	if(strs != null) {
    		for (String string : strs) {
    			buffer.append(string);
    		}
    	}
    	return buffer.toString();    		
    }
}
