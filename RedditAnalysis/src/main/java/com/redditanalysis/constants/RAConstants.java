package com.redditanalysis.constants;

public enum RAConstants {

	ONE (1),
	
	// Property file keys
	BASE_LOC ( "BASE_LOC"),
	INBOX ( "INBOX"),

	CTL_FILE_EXT ( "CTL_FILE_EXT"),
	META_FILE_EXT ( "META_FILE_EXT"),
	DAT_FILE_EXT ( "DAT_FILE_EXT"),

	HDFS_CONTROL_FOLDER_NAME ( "CONTROL_FOLDER_NAME"),
	HDFS_META_FOLDER_NAME ( "META_FOLDER_NAME"),
	HDFS_DATA_FOLDER_NAME ( "DATA_FOLDER_NAME"),

	// File Extensions
	EXTENSION_CTL ( "ctl"),
	EXTENSION_META ( "meta"),
	EXTENSION_DATA ( "dat"),

	
	
	// Separators 
	SEPERATOR_PIPE ( "|"),
	SEPERATOR_TAB ( "\t"),
	SEPERATOR_COMMA ( ","),
	SEPERATOR_EQUAL ( "("),
	SEPERATOR_DOT ( "."),
	FALSE ( "N"),
	EMPTY ( ""),
	ZERO ( 0),
	
	//Control File Indexes
	CTL_SOURCENAME_IDX(1),
	CTL_SOURCESCHEMANAME_IDX(2),
	CTL_ENTITYNAME_IDX(3),
	CTL_RECORDCOUNT_IDX(4),
	CTL_EXTRACTDATE_IDX(5),
	CTL_EXTRACTTIME_IDX(6),
	CTL_FILETIMESTAMP_IDX(7),
	CTL_EXTRACTDATETIME_IDX(8),
	CTL_ETLLANDINGDIRECTORY_IDX(9),
	CTL_FILELOADTYPE_IDX(10),
	CTL_ETLAPPUSERID_IDX(11),
	CTL_EXTRACTSTATUS_IDX(12),
	CTL_SOURCETIMEZONE_IDX(13),
	CTL_EFFECTIVEDATE_IDX(14),
	CTL_DATAFILEEXTENSION_IDX(15),
	CTL_ADDITIONALFILESEXTENSIONS_IDX(16),
	CTL_FILEFORMATTYPE_IDX(17),
	CTL_FIELDDELIMITER_IDX(18),
	CTL_SOURCEWATERMARK_1_IDX(19),
	CTL_SOURCEWATERMARK_2_IDX(20),
	CTL_ETLPROCESS_IDX(21),
	CTL_FREQUENCY_IDX(22),
	CTL_RECORDDELIMITER_IDX(23),

	TRIGGER_FILE_NAME_EXT ( "TRIGGER_FILE_NAME_EXT"),

	FILE_SEPARATOR_NAME ( "file.separator"),
	FILE_SEPARATOR(System.getProperty("file.separator")),

	HDFS_BASE_LOC ( "HDFS_BASE_LOC"),
	ARCHIVE_LOC ( "ARCHIVE"),
	FAILED_LOC ( "FAILED"),

	PROPERTY_FILE_NAME ( "common.properties"),

	PROJECT_TIME_FORMAT ( "yyyyMMdd"),

	HDFS_INBOX_LOC ( "HDFS_INBOX"),

	DESTINATION_FORMAT ( "DESTINATION_FORMAT"),

	CLASS_AVRO_FORMAT ( "org.avroservice.AvroStorageFormat"),

	DESTINATION_FORMAT_AVRO ( "avro"), // Important Note::: The value is in properties file name for  PROPERTY_FILE_NAME key DESTINATION_FORMAT

		TYPE("type"),
		TYPE_RECORD("record"),
		TABLE_NAME("name"),
		SOURCE_NAME("source"),
		NAMESPACE("namespace"),
		DOC("doc"),
		FIELDS("fields"),
		FIELD_NAME("name"),
		FIELD_DOC("doc"),
		FIELD_TYPE("type"),
		FIELD_DEFAULT("default"),
		
		
		//Columns
		FIELD_SOURCENAME("sourceName"),
		FIELD_SCHEMANAME("schemaName"),
		FIELD_TABLENAME("tableName"),
		FIELD_COLUMNNAME("columnName"),
		FIELD_DATATYPE("dataType"),
		FIELD_DATALENGTH("dataLength"),
		FIELD_DATASCALE("dataScale"),
		FIELD_FORMAT("format"),
		FIELD_PRIMARYKEY("primaryKey"),
		FIELD_COLUMNID("columnId");		
		
		
		
		private String value;
		private int intValue;
		
		private RAConstants(final String value)
		{
			this.value=value;
		}
		
		private RAConstants(final int intValue)
		{
			this.intValue=intValue;
		}
		
		public String getValue(){
			return this.value;
		}
		
		public int getIntValue(){
			return this.intValue;
		}
	
}
