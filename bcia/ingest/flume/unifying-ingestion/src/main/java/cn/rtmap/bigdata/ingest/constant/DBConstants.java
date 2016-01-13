package cn.rtmap.bigdata.ingest.constant;

public final class DBConstants {

	public static final String CONFIG_PROPS               = "properties";
	public static final String CONFIG_JDBC_DRIVER         = "jdbc.driver";
	public static final String CONFIG_CONNECTION_URL      = "db.url";
	public static final String CONFIG_CONNECTION_USERNAME = "db.username";
	public static final String CONFIG_CONNECTION_PASSWORD = "db.password";
	public static final String CONFIG_CRON_EXPRESS        = "cron.express";
	public static final String CONFIG_QUERY_KEYPRE        = "table.";
	public static final String CONFIG_QUERY_STARTDATE     = "rpfield_start_date";
	public static final String CONFIG_QUERY_ENDDATE       = "rpfield_end_date";
	public static final String CONFIG_DB_CONFS            = "db.conf";
	
	public static final String  CONFIG_TMP_EXTENSION      = ".tmp";
	public static final String  CONFIG_ZIP_EXTENSION      = ".zip";
	public static final String  CONFIG_FILE_EXTENSION     = ".dat";
	
	public static final String  CONFIG_DEBUG              = "debugger";
	
	private DBConstants() {
		// Disable explicit creation of objects.
	}

}
