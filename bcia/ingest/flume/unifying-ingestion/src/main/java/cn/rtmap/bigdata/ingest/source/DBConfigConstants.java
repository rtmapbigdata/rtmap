package cn.rtmap.bigdata.ingest.source;

public final class DBConfigConstants {

	public static final String CONFIG_PROPS               = "properties";
	public static final String CONFIG_JDBC_DRIVER         = "jdbc.driver";
	public static final String CONFIG_CONNECTION_URL      = "db.url";
	public static final String CONFIG_CONNECTION_USERNAME = "db.username";
	public static final String CONFIG_CONNECTION_PASSWORD = "db.password";
	public static final String CONFIG_CRON_EXPRESS        = "cron.express";
	public static final String CONFIG_QUERY_KEYPRE        = "table.";
	public static final String CONFIG_QUERY_STARTDATE     = "field_start_date";
	public static final String CONFIG_QUERY_ENDDATE       = "field_end_date";
	public static final String CONFIG_DB_CONFS     = "db.conf";
	
	private DBConfigConstants() {
		// Disable explicit creation of objects.
	}

}
