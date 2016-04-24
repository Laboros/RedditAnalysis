package com.redditanalysis.cron;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * 
 * @author svaduka
 * {@link RACleanUpJob} is used to cleanup the following:
 * 1) Cleanup inbox,archive,failed folders in local file system
 * 2) Cleanup inbox,ready,processed folders in HDFS file system
 * 3) Remove the control file audit tables
 * 4) Stops any Scheduler tasks 
 * 
 * It will also used to create following.
 * 1) Creates new inbox,archive,failed folders in local file system
 * 2) Creates new inbox,ready,processed folders in HDFS file system
 * 3) Create audit/control tables
 * 4) Creates the scheduler tasks
 * 
 */
public class RACleanUpJob extends Configured implements Tool {

	public static void main(String[] args) {
		
	}
	
	public int run(String[] args) throws Exception {
		return 0;
	}
}
