package cn.rtmap.bigdata.ingest.impl;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.source.DBConfigConstants;
import cn.rtmap.bigdata.ingest.source.FileSourceConfigurationConstants;
import cn.rtmap.bigdata.ingest.utils.DateUtils;
import cn.rtmap.bigdata.ingest.utils.SQLUtil;

public class DBExtractor {
	private static final Logger logger = LoggerFactory.getLogger(DBExtractor.class);
	private Connection connection = null;
	Statement statement = null;
	Map<String,String> querys = new HashMap<String,String>();
	private String incomingDir = null;
	boolean debugger = false;
	
	public void init(Context ctx,Properties properties) throws Exception{
		String driver=properties.getProperty(DBConfigConstants.CONFIG_JDBC_DRIVER).trim();
		String url=properties.getProperty(DBConfigConstants.CONFIG_CONNECTION_URL).trim();
		String username=properties.getProperty(DBConfigConstants.CONFIG_CONNECTION_USERNAME).trim();
		String password=properties.getProperty(DBConfigConstants.CONFIG_CONNECTION_PASSWORD).trim();
		connection = SQLUtil.getConnection(driver,url,username,password);
		Enumeration<?> keys = properties.propertyNames();
		while(keys.hasMoreElements()){
			String key = (String) keys.nextElement();
			if(key.startsWith(DBConfigConstants.CONFIG_QUERY_KEYPRE)){
				querys.put(key.substring(DBConfigConstants.CONFIG_QUERY_KEYPRE.length()),properties.getProperty(key));
			}
		}
		incomingDir = properties.getProperty(FileSourceConfigurationConstants.CONFIG_INCOMING_DIR);
		String debug=properties.getProperty(DBConfigConstants.CONFIG_DEBUG);
		if(debug != null && ("true".equals(debug) || "1".equals(debug))){
			debugger=true;
		}
		logger.info("Extractor start with " + querys.size() + " tables: " + properties.getProperty(DBConfigConstants.CONFIG_CONNECTION_URL));
	}
	
	public void extract() throws Exception {
		statement = connection.createStatement();
		String today = DateUtils.getCurrentDate();
		String yesterday = DateUtils.getDateByCondition(-1);
		for(String table:querys.keySet()){
			String sql = querys.get(table)
					.replaceAll(DBConfigConstants.CONFIG_QUERY_STARTDATE, yesterday+" 00:00:00")
					.replaceAll(DBConfigConstants.CONFIG_QUERY_ENDDATE, today+" 00:00:00");
			if(debugger){
				sql += " limit 10";
			}
			//file name
			String fileName = yesterday.replaceAll("-", "") +  "_" + table.replaceAll("_", "-");
			if(querys.get(table).contains(DBConfigConstants.CONFIG_QUERY_STARTDATE)){
				fileName = "a_" + fileName;
			}else{
				fileName = "i_" + fileName;
			}
			String filePath = incomingDir + "/" + yesterday.replaceAll("-", "") + "/day/" + fileName;
			String tmpFile  = filePath + DBConfigConstants.CONFIG_TMP_EXTENSION;
			ResultSet rs = null;
			try {
				logger.info("start sql: " + sql);
				rs = statement.executeQuery(sql);
				boolean succ = SQLUtil.saveResultSetAsCSV(rs, tmpFile);
				if(succ){
					SQLUtil.createVerfFile(new File(tmpFile), fileName+DBConfigConstants.CONFIG_FILE_EXTENSION, 
							filePath+FileSourceConfigurationConstants.DEFAULT_VERF_EXTENSION);
					SQLUtil.zipFile(tmpFile,filePath+DBConfigConstants.CONFIG_ZIP_EXTENSION,true);
				}
				//new File(tmpFile).deleteOnExit();
				logger.info("finish sql: "+ filePath);
			} catch (Exception e) {
				logger.error("Table Extract Fail >>> " + table,e);
			}finally{
				if(rs != null){rs.close();}
			}
		}
	}
	
	public void close() {
		SQLUtil.close(connection, statement);
	}
	
}
