package com.lobstar.config;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 运行参数，设置为可在运行期间改变
 * 在构建参数之后初始化
 * @author lobster
 *
 */
public class RuntimeConfiguration {
	private ScheduledExecutorService poolExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
	
	public void init()
	{
		poolExecutor.scheduleWithFixedDelay(new ConfigListener(), 0, 5, TimeUnit.SECONDS);
	}
	
	private class ConfigListener extends Thread {
		@Override
		public void run() {
		}
	}
}
