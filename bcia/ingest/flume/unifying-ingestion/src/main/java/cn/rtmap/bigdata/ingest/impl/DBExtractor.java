package cn.rtmap.bigdata.ingest.impl;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
			statement = connection.createStatement();
			ImmutableSet<String> keys=ctx.getParameters().keySet();
			for(String key : keys){
				if(key.startsWith(DBConstants.CONFIG_QUERY_KEYPRE)){
					querys.put(key.substring(DBConstants.CONFIG_QUERY_KEYPRE.length()),ctx.getString(key));
				}
			}
			incomingDir = ctx.getString(CommonConstants.CONFIG_INCOMING_DIR);
			logger.info("init finish with " + querys.size() + " tables: " + url);
		} catch (Exception e) {
			logger.error("create connection or statement error: "+e.getLocalizedMessage(),e);
		}
	}
	
	@Override
	public Iterator<JsonElement<String, String>> getData() {
		if(statement != null){
			extract();
		}else{
			logger.error("statement create fail, extract cancel!");
		}
		return null;
	}
	
	private void extract(){
		String today = DateUtils.getCurrentDate();
		String yesterday = DateUtils.getDateByCondition(-1);
		String datetime1 = yesterday + " 00:00:00";
		String datetime2 = today + " 00:00:00";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long timestamp1=0;
		long timestamp2=0;
		try {
			timestamp1 = sdf.parse(datetime1).getTime();
			timestamp2 = sdf.parse(datetime2).getTime();
		} catch (ParseException e) {
			logger.error("init datetime value error",e);
		}
		for(String table : querys.keySet()){
			String cfgSql = querys.get(table);
			String sql = cfgSql.replaceAll(DBConstants.CONFIG_QUERY_STARTDATE, datetime1)
					        .replaceAll(DBConstants.CONFIG_QUERY_ENDDATE, datetime2)
					        .replaceAll(DBConstants.CONFIG_SQL_STARTTIME_LONG, timestamp1+"")
					        .replaceAll(DBConstants.CONFIG_SQL_ENDTIME_LONG, timestamp2+"");
			if(debugger){
				sql += " limit 10";
			}
			String fileName = yesterday.replaceAll("-", "") +  "_" + table.replaceAll("_", "-");
			if(cfgSql.contains(DBConstants.CONFIG_QUERY_STARTDATE) || cfgSql.contains(DBConstants.CONFIG_SQL_STARTTIME_LONG)){
				fileName = "a_" + fileName;
			}else{
				fileName = "i_" + fileName;
			}
			String filePath = incomingDir + "/" + yesterday.replaceAll("-", "") + "/day/" + fileName;
			String tmpFile  = filePath + CommonConstants.DEFAULT_TMP_EXTENSION;
			ResultSet rs = null;
			try {
				logger.info("start execute query: " + sql);
				rs = statement.executeQuery(sql);
				boolean succ = SQLUtil.saveResultSetAsCSV(rs, tmpFile);
				if(succ){
					String tmpVerf=filePath + FileSourceConfigurationConstants.DEFAULT_VERF_EXTENSION + CommonConstants.DEFAULT_TMP_EXTENSION;
					SQLUtil.createVerfFile(new File(tmpFile), fileName+CommonConstants.DEFAULT_FILE_EXTENSION, tmpVerf);
					SQLUtil.zipFile(tmpFile,filePath+CommonConstants.DEFAULT_ZIP_EXTENSION,true);
					new File(tmpVerf).renameTo(new File(filePath + FileSourceConfigurationConstants.DEFAULT_VERF_EXTENSION));
				}
				logger.info("finish query: " + filePath);
			} catch (Exception e) {
				logger.error("table extract fail, " + table,e);
			}finally{
				try {
					if(rs != null){rs.close();}
				} catch (SQLException e) {
					logger.error("resultset close error, " + table,e);
				}
			}
		}
	}
	
	@Override
	public void cleanup() {
		SQLUtil.close(connection, statement);
		logger.info("---------- finish job ----------");
	}
	
	public static void main(String[] args) {
		try {
			String fileName="db-market";
			String filePath="C:\\Users\\zxw\\Desktop\\temp\\temp\\" + fileName;
			String tmpFile  = filePath + CommonConstants.DEFAULT_TMP_EXTENSION;
			String tmpVerf=filePath + FileSourceConfigurationConstants.DEFAULT_VERF_EXTENSION + CommonConstants.DEFAULT_TMP_EXTENSION;
			SQLUtil.createVerfFile(new File(tmpFile), fileName+CommonConstants.DEFAULT_FILE_EXTENSION, tmpVerf);
			System.out.println("tmp verf");
			Thread.sleep(5000);
			SQLUtil.zipFile(tmpFile,filePath+CommonConstants.DEFAULT_ZIP_EXTENSION,true);
			System.out.println("zip");
			Thread.sleep(5000);
			new File(tmpVerf).renameTo(new File(filePath + FileSourceConfigurationConstants.DEFAULT_VERF_EXTENSION));
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}
