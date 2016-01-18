package cn.rtmap.bigdata.ingest.recover;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.constant.DBConstants;
import cn.rtmap.bigdata.ingest.source.FileSourceConfigurationConstants;
import cn.rtmap.bigdata.ingest.utils.DateUtils;
import cn.rtmap.bigdata.ingest.utils.SQLUtil;

public class DBExtractRecover{
	private Connection connection = null;
	private Statement statement = null;
	private Map<String,String> querys = new HashMap<String,String>();
	private String incomingDir = null;
	
	public void init(String properFile){
		try {
			Properties properties = new Properties();
			InputStream inputStream = new FileInputStream(properFile);
			properties.load(inputStream);
			String driver=properties.getProperty(DBConstants.CONFIG_JDBC_DRIVER).trim();
			String url=properties.getProperty(DBConstants.CONFIG_CONNECTION_URL).trim();
			String username=properties.getProperty(DBConstants.CONFIG_CONNECTION_USERNAME).trim();
			String password=properties.getProperty(DBConstants.CONFIG_CONNECTION_PASSWORD).trim();
			connection = SQLUtil.getConnection(driver,url,username,password);
			statement = connection.createStatement();
			Set<Object> keys=properties.keySet();
			for(Object key1 : keys){
				String key=key1.toString();
				if(key.startsWith(DBConstants.CONFIG_QUERY_KEYPRE)){
					querys.put(key.substring(DBConstants.CONFIG_QUERY_KEYPRE.length()),properties.getProperty(key));
				}
			}
			incomingDir = properties.getProperty(CommonConstants.CONFIG_INCOMING_DIR);
			System.out.println("init finish with " + querys.size() + " tables: " + url);
		} catch (Exception e) {
			System.out.println("create connection or statement error: "+e.getLocalizedMessage());
		}
	}
	
	public void getData(String date) throws ParseException {
		if(statement != null){
			extract(date);
		}else{
			System.out.println("statement create fail, extract cancel!");
		}
	}
	
	private void extract(String date) throws ParseException{
		String today = DateUtils.getDateByCondition(1, date);
		String yesterday = date;
		String datetime1 = yesterday + " 00:00:00";
		String datetime2 = today + " 00:00:00";
		System.out.println("start: "+yesterday+", end: "+today);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long timestamp1=0;
		long timestamp2=0;
		try {
			timestamp1 = sdf.parse(datetime1).getTime();
			timestamp2 = sdf.parse(datetime2).getTime();
		} catch (ParseException e) {
			System.out.println("init datetime value error");
		}
		for(String table : querys.keySet()){
			String cfgSql = querys.get(table);
			String sql = cfgSql.replaceAll(DBConstants.CONFIG_QUERY_STARTDATE, datetime1)
					        .replaceAll(DBConstants.CONFIG_QUERY_ENDDATE, datetime2)
					        .replaceAll(DBConstants.CONFIG_SQL_STARTTIME_LONG, timestamp1+"")
					        .replaceAll(DBConstants.CONFIG_SQL_ENDTIME_LONG, timestamp2+"");
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
				System.out.println("start execute query: " + sql);
				rs = statement.executeQuery(sql);
				boolean succ = SQLUtil.saveResultSetAsCSV(rs, tmpFile);
				if(succ){
					SQLUtil.createVerfFile(new File(tmpFile), fileName+CommonConstants.DEFAULT_FILE_EXTENSION, 
							filePath+FileSourceConfigurationConstants.DEFAULT_VERF_EXTENSION);
					SQLUtil.zipFile(tmpFile,filePath+CommonConstants.DEFAULT_ZIP_EXTENSION,true);
				}
				System.out.println("finish query: " + filePath);
			} catch (Exception e) {
				System.out.println("table extract fail, " + table);
			}finally{
				try {
					if(rs != null){rs.close();}
				} catch (SQLException e) {
					System.out.println("resultset close error, " + table);
				}
			}
		}
	}
	
	public void cleanup() {
		SQLUtil.close(connection, statement);
		System.out.println("---------- finish job ----------");
	}
	
	public static void main(String[] args) {
		try {
			System.out.println("parameter: <properties> <date>");
			DBExtractRecover extractor = new DBExtractRecover();
			extractor.init(args[0]);
			extractor.getData(args[1]);
			extractor.cleanup();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
