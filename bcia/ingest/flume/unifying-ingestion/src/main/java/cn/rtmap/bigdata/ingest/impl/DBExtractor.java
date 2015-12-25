package cn.rtmap.bigdata.ingest.impl;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
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
		logger.info("Extractor " + querys.size() + " tables: " + properties.getProperty(DBConfigConstants.CONFIG_CONNECTION_URL));
		incomingDir = properties.getProperty(FileSourceConfigurationConstants.CONFIG_INCOMING_DIR);
		
	}
	
	public void extract() throws Exception {
		statement = connection.createStatement();
		String today = DateUtils.getCurrentDate();
		String yesterday = DateUtils.getDateByCondition(-1);
		for(String table:querys.keySet()){
			String sql = querys.get(table)
					.replaceAll(DBConfigConstants.CONFIG_QUERY_STARTDATE, yesterday+" 00:00:00")
					.replaceAll(DBConfigConstants.CONFIG_QUERY_ENDDATE, today+" 00:00:00");
			String fileName = table + "_" + yesterday + ".txt";
			String filePath = incomingDir + "/" + fileName;
			String tmpFile  = filePath + ".tmp";
			logger.info(sql);
			ResultSet rs = statement.executeQuery(sql);
			SQLUtil.saveResultSetAsCSV(rs, tmpFile);
			logger.info(tmpFile);
			File srcFile = new File(tmpFile);
			if(srcFile.exists()){
				File destFile = new File(filePath);
				if(destFile.exists()){
					logger.warn("extract file cannot be upload,will be replaced: " + filePath);
					destFile.deleteOnExit();
				}
				srcFile.renameTo(new File(filePath));
			}else{
				logger.error("table extract fail,file not exist: " + table);
			}
		}
	}
	
	public void close() {
		SQLUtil.close(connection, statement);
	}
	
}
