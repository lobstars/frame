package com.lobstar.context;

import com.lobstar.config.Builder;

public class MasterContext {
	private static Builder builder;

	public static Builder getBuilder() {
		return builder;
	}

	public static void setBuilder(Builder builder) {
		MasterContext.builder = builder;
	}
	
}
