package com.lobstar.base.exception;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class ExceptionTools {

    private static String lineSeparator = java.security.AccessController
            .doPrivileged(new sun.security.action.GetPropertyAction("line.separator"));

    public static String getExceptionStack(StackTraceElement[] stackTraceElements) {

        return getExceptionStack(stackTraceElements, 50);
    }

    public static String getExceptionStack(Exception e) {
    	ByteArrayOutputStream out = null;
    	PrintWriter writer = null;
    	try {
    		out = new ByteArrayOutputStream();
    		writer = new PrintWriter(out);
    		e.printStackTrace(writer);
    		
    		writer.flush();
    		
    		return out.toString();    		
    	}finally {
    		if(writer != null) {
    			writer.close();
    		}
    		if(out != null) {
    			try {
					out.close();
				} catch (IOException e1) {
				}
    		}
    	}
    }

    private static String getExceptionStack(StackTraceElement[] stackTraceElements, int threshold) {
        int num = stackTraceElements.length;

        int line = Math.min(num, threshold);
        StringBuffer ret = new StringBuffer();
        for (int i = 0; i < line; i++) {
            StackTraceElement stackTraceElement = stackTraceElements[i];
            ret.append(stackTraceElement.getClassName());
            ret.append(".");
            ret.append(stackTraceElement.getMethodName());
            ret.append("(line:");
            ret.append(stackTraceElement.getLineNumber());
            ret.append(")" + lineSeparator);
        }

        return ret.toString();
    }
}
