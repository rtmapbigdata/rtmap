package cn.rtmap.bigdata.ingest.impl;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.base.AbstractExtractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.constant.DBConstants;
import cn.rtmap.bigdata.ingest.source.FileSourceConfigurationConstants;
import cn.rtmap.bigdata.ingest.utils.DateUtils;
import cn.rtmap.bigdata.ingest.utils.SQLUtil;

import com.google.common.collect.ImmutableSet;

public class DBExtractor extends AbstractExtractor{
	private static final Logger logger = LoggerFactory.getLogger(DBExtractor.class);
	private Connection connection = null;
	private Statement statement = null;
	private Map<String,String> querys = new HashMap<String,String>();
	private String incomingDir = null;
	
	@Override
	public void init(Context ctx){
		super.init(ctx);
		String driver=ctx.getString(DBConstants.CONFIG_JDBC_DRIVER).trim();
		String url=ctx.getString(DBConstants.CONFIG_CONNECTION_URL).trim();
		String username=ctx.getString(DBConstants.CONFIG_CONNECTION_USERNAME).trim();
		String password=ctx.getString(DBConstants.CONFIG_CONNECTION_PASSWORD).trim();
		try {
			connection = SQLUtil.getConnection(driver,url,username,password);
		} catch (Exception e) {
			logger.error("create connection error: "+e.getLocalizedMessage(),e);
		}
		ImmutableSet<String> keys=ctx.getParameters().keySet();
		for(String key: keys){
			if(key.startsWith(DBConstants.CONFIG_QUERY_KEYPRE)){
				querys.put(key.substring(DBConstants.CONFIG_QUERY_KEYPRE.length()),ctx.getString(key));
			}
		}
		incomingDir = ctx.getString(CommonConstants.CONFIG_INCOMING_DIR);
		logger.info("init finish with " + querys.size() + " tables: " + url);
	}
	
	@Override
	public Iterator<JsonElement<String, String>> getData() {
		if(connection == null){
			logger.error("connection fail,extract cancel!");
			return null;
		}
		try {
			statement = connection.createStatement();
			extract();
		} catch (SQLException e) {
			logger.error("create statement error",e);
		}
		return null;
	}
	
	public void extract(){
		String today = DateUtils.getCurrentDate();
		String yesterday = DateUtils.getDateByCondition(-1);
		for(String table:querys.keySet()){
			String sql = querys.get(table).replaceAll(DBConstants.CONFIG_QUERY_STARTDATE, yesterday+" 00:00:00")
					           .replaceAll(DBConstants.CONFIG_QUERY_ENDDATE, today+" 00:00:00");
			if(debugger){
				sql += " limit 10";
			}
			//file name
			String fileName = yesterday.replaceAll("-", "") +  "_" + table.replaceAll("_", "-");
			if(querys.get(table).contains(DBConstants.CONFIG_QUERY_STARTDATE)){
				fileName = "a_" + fileName;
			}else{
				fileName = "i_" + fileName;
			}
			String filePath = incomingDir + "/" + yesterday.replaceAll("-", "") + "/day/" + fileName;
			String tmpFile  = filePath + DBConstants.CONFIG_TMP_EXTENSION;
			ResultSet rs = null;
			try {
				logger.info("start sql: " + sql);
				rs = statement.executeQuery(sql);
				boolean succ = SQLUtil.saveResultSetAsCSV(rs, tmpFile);
				if(succ){
					SQLUtil.createVerfFile(new File(tmpFile), fileName+DBConstants.CONFIG_FILE_EXTENSION, 
							filePath+FileSourceConfigurationConstants.DEFAULT_VERF_EXTENSION);
					SQLUtil.zipFile(tmpFile,filePath+DBConstants.CONFIG_ZIP_EXTENSION,true);
				}
				logger.info("finish sql: "+ filePath);
			} catch (Exception e) {
				logger.error("Table Extract Fail >>> " + table,e);
			}finally{
				if(rs != null){
					try {
						rs.close();
					} catch (SQLException e) {
						logger.error("ResultSet close error," + table,e);
					}
				}
			}
		}
	}
	
	@Override
	public void cleanup() {
		SQLUtil.close(connection, statement);
	}

}
