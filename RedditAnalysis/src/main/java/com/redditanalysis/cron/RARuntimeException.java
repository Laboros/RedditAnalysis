package com.redditanalysis.cron;

import com.commonservice.util.LoggerUtil;


public class RARuntimeException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	private final LoggerUtil util=new LoggerUtil(RARuntimeException.class);
	
	public RARuntimeException(final String msg){
		logMessage(msg);
		
	}
	
	public void logMessage(final String msg)
	{
		util.error(msg);
	}

}
