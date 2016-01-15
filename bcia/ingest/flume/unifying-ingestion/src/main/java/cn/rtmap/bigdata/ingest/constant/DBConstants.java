package cn.rtmap.bigdata.ingest.constant;

public final class DBConstants {
	public static final String CONFIG_JDBC_DRIVER         = "jdbc.driver";
	public static final String CONFIG_CONNECTION_URL      = "db.url";
	public static final String CONFIG_CONNECTION_USERNAME = "db.username";
	public static final String CONFIG_CONNECTION_PASSWORD = "db.password";
	public static final String  CONFIG_HOST               = "host";
	public static final String  CONFIG_PORT               = "port";
	public static final String  CONFIG_DB                 = "database";
	
	public static final String CONFIG_DB_CONFS            = "db.conf";
	public static final String CONFIG_QUERY_KEYPRE        = "table.";
	public static final String CONFIG_QUERY_STARTDATE     = "rpfield_start_date";
	public static final String CONFIG_QUERY_ENDDATE       = "rpfield_end_date";
	public static final String CONFIG_SQL_STARTTIME_LONG  = "rplong_start_time";
	public static final String CONFIG_SQL_ENDTIME_LONG    = "rplong_end_time";
	
	public static final String  SIGN_TAB      = "\t";
	public static final String  SIGN_RET_R    = "\r";
	public static final String  SIGN_RET_N    = "\n";
	public static final String  REPLACE_TAB   = "@tab";
	public static final String  REPLACE_RET_R = "@ret_r";
	public static final String  REPLACE_RET_N = "@ret_n";
	
	private DBConstants() {
		// Disable explicit creation of objects.
	}

}
