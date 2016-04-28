package com.redditanalysis.cron;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hdfsservice.util.HDFSUtil;

import com.commonservice.FileUtil;
import com.commonservice.util.DateUtil;
import com.commonservice.util.PropertyReader;
import com.redditanalysis.constants.RAConstants;

/**
 * 
 * @author svaduka
 * This class will be used for CRON. It will continuous monitor on the inbox location specified in the common.properties
 * BASE_LOC+\+INBOX.
 * It will keep on look for xml files placed in the above location
 * Once the xml file is placed inside the inbox location the data will be moved to hdfs location and do a control process audit operation with the details about the file ingested
 * After successfully completion on moving xml file into hdfs inbox location, the xml file either moved to archive folder or failed folder.
 */
public class RACronJob extends Configured implements Tool{
	
	//The process time is constant for all processed files
	private long processStartTime=-1l;

	//Class used to read the property files
	public static PropertyReader propReader = null; 
	
	public static void main(String[] args) throws IOException {

		// Read Properties

		propReader = new PropertyReader(RAConstants.PROPERTY_FILE_NAME.getValue());
		boolean propertiesLoaded = propReader.loadProperties();

		if (propertiesLoaded) {

			try {
				Configuration conf = new Configuration(Boolean.TRUE);
				conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
				int i = ToolRunner.run(conf, new RACronJob(), args);
				if (i == 0) {
					System.out.println("Success");
				} else {
					System.out.println("Failed");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * 
	 * @param lookupFile
	 * @param processFiles
	 * @return TRUE/FALSE indicates the files are moved successfully
	 * This method will move the files in list to failed location
	 */
	
	public  boolean processMoveFilesToArchive(final String lookupFile, final List<String> processFiles) {
		
		boolean procesedFiles=Boolean.TRUE;
		
		final String BASE_LOCATION=propReader.getValue(RAConstants.BASE_LOC.getValue());
		final String archiveLoc=propReader.getValue(RAConstants.ARCHIVE_LOC.getValue());
		final String destinationLoc=BASE_LOCATION+RAConstants.FILE_SEPARATOR.getValue()+archiveLoc;

		procesedFiles=processMoveFilesToDestination(destinationLoc, processFiles);
		return procesedFiles;
		
	}

	/**
	 * 
	 * @param lookupFile
	 * @param processFiles
	 * @return TRUE/FALSE indicates the files are moved successfully
	 * This method will move the files in list to failed location
	 */
	public  boolean processMoveFilesToFailed(final String lookupFile, final List<String> processFiles) {
		boolean procesedFiles=Boolean.TRUE;
		
		final String BASE_LOCATION=propReader.getValue(RAConstants.BASE_LOC.getValue());
		final String failedLoc=propReader.getValue(RAConstants.FAILED_LOC.getValue());
		final String destinationLoc=BASE_LOCATION+RAConstants.FILE_SEPARATOR.getValue()+failedLoc;

		procesedFiles=processMoveFilesToDestination(destinationLoc, processFiles);
		
		return procesedFiles;
	}
	
	/**
	 * 
	 * @param destinationLoc
	 * @param processFiles
	 * @return TRUE/FALSE indicates the files are moved successfully
	 * 
	 * This method will move the files in list to destination location
	 */
	
	public  boolean processMoveFilesToDestination(final String destinationLoc,final List<String> processFiles) {
		
		boolean procesedFiles=Boolean.TRUE;
		
		try
		{
			for (String fileNameWithLoc : processFiles) 
			{
				FileUtil.moveFileToLoc(fileNameWithLoc,destinationLoc);
			}
		}catch (IOException e) {
			procesedFiles=Boolean.FALSE;
		}
		return procesedFiles;
		
	}

	public int run(String[] args) throws Exception {

		while (Boolean.TRUE) {
			
			processStartTime=System.currentTimeMillis();  // This is the time through out the current process
			
			boolean anyFilesProcessed=process(args);
			if(anyFilesProcessed){
				System.out.println("Some Files are processed please check in the archive dirctory with current timestamp");
			}else{
				System.out.println("Waiting....");
			}
		}
	

	return 0;
}
	/**
	 * 
	 * @param args
	 * @return TRUE/FALSE indicates the files inside the inbox location is processed or not
	 * @throws IOException
	 */
	public boolean process(final String[] args) throws IOException {
		
		boolean isAnyFilesProcessed=Boolean.FALSE;
		
		final String BASE_LOC = propReader.getValue(RAConstants.BASE_LOC.getValue());
		final String INBOX_LOC=BASE_LOC+RAConstants.FILE_SEPARATOR.getValue()+propReader.getValue(RAConstants.INBOX.getValue());
		
		final String triggerExt=propReader.getValue(RAConstants.TRIGGER_FILE_NAME_EXT.getValue()); // XML file lookup

		Map<String, List<String>> groupedFiles = FileUtil.groupSimilarFiles(INBOX_LOC,triggerExt);
		
		
		if(groupedFiles!=null && !groupedFiles.isEmpty()){
			
			for (Map.Entry<String, List<String>> groupFile : groupedFiles.entrySet()) 
			{
				final String lookupFileName=groupFile.getKey(); 
				final List<String> processFiles=groupFile.getValue();
				
				boolean filesProcessed=processIndividualGroupFile(lookupFileName, processFiles);
				
				if(filesProcessed){
					System.out.println("Files processed for :"+lookupFileName);
				}else{
					throw new RARuntimeException("Unable to process for file with lookup Name:"+lookupFileName);
				}
			}
			
			isAnyFilesProcessed=Boolean.TRUE;
		}
		return isAnyFilesProcessed;
	}
	
	/**
	 * 
	 * @param lookupFile
	 * @param processFiles
	 * @return TRUE/FALSE indicates the individual file group processed or not
	 * @throws IOException
	 */

	public boolean processIndividualGroupFile(final String lookupFile, final List<String> processFiles) throws IOException
	{
		boolean isFilesMovedToHDFS=processMoveFilesToHDFS(lookupFile, processFiles);
		
		if(isFilesMovedToHDFS)
		{
			boolean isFileMovedToArchive=processMoveFilesToArchive(lookupFile, processFiles);
			if(!isFileMovedToArchive)
			{
				throw new RARuntimeException("Process failed: move file to archive location, look up file name:"+lookupFile);
			}
			
		}else{
			boolean isFilesMovedToFailed=processMoveFilesToFailed(lookupFile, processFiles);
			if(!isFilesMovedToFailed)
			{
				throw new RARuntimeException("Process failed: move file to failed location, look up file name:"+lookupFile);
			}
			
		}
		
		return Boolean.TRUE;
	}

	/**
	 * This method will move the corresponding lookupfile entries from inbox location to respective HDFS location.
	 * If moving HDFS files fail, then the inbox files will move to failed directory.
	 * 
	 * @param lookupFile
	 * @param processFiles
	 * @return TRUE/FALSE indicates the list of files moved to hdfs or not
	 * @throws IOException
	 */
	public boolean processMoveFilesToHDFS(final String lookupFile, final List<String> processFiles) {
		
		boolean isFilesMovedTOHDFS=Boolean.TRUE;
		
		final Configuration conf=super.getConf();
		System.out.println(conf.get("fs.defaultFS"));
		
		//HDFS base location from property file
		final String hdfsbaseLoc=propReader.getValue(RAConstants.HDFS_BASE_LOC.getValue());
		final String xmlFileLoc = FileUtil.getExtFile(processFiles,propReader.getValue(RAConstants.DAT_FILE_EXT.getValue()));
		final String hdfsXMLFileLoc = hdfsbaseLoc
							+RAConstants.FILE_SEPARATOR.getValue()
							+propReader.getValue(RAConstants.HDFS_DATA_FOLDER_NAME.getValue())
							+RAConstants.FILE_SEPARATOR.getValue()
							+propReader.getValue(RAConstants.HDFS_INBOX_LOC.getValue())
							+RAConstants.FILE_SEPARATOR.getValue()
							+DateUtil.convertTimeIntoFormat(processStartTime,propReader.getValue(RAConstants.PROJECT_TIME_FORMAT.getValue()));

		System.out.println("DAT FILE LOC:" + hdfsXMLFileLoc);

		isFilesMovedTOHDFS = HDFSUtil.writeLocalFileOnHDFS(xmlFileLoc,hdfsXMLFileLoc, conf);
		return isFilesMovedTOHDFS;
	}
}
