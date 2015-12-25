package cn.rtmap.bigdata.ingest.utils;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;

public class SQLUtil {
	private static final Logger logger = LoggerFactory.getLogger(SQLUtil.class);
	
	public static void dump(Connection connection, List<String> querys, String incomingDir) throws Exception {
		Statement statement = connection.createStatement();
		for(String sql:querys){
			String file = incomingDir + "data.txt";
			String export = sql + " into outfile '" + file + "'";
			logger.info(export);
			statement.executeQuery(export);
		}
	}
	
	public static void saveResultSetAsCSV(ResultSet rs, String fileName) throws Exception {
		if(rs == null || fileName == null){
			logger.error("Save ResultSet As CSV Error: ResultSet or fileName is null");
			return;
		}
		List<String[]> allRows = new ArrayList<String[]>();
		String[] row=null;
		ResultSetMetaData meta = rs.getMetaData();
		int num = meta.getColumnCount();
		while (rs.next()) {
			row = new String[num];
			for (int i = 1; i <= num; ++i) {
				if (rs.getObject(i) != null){
					row[i-1] = rs.getObject(i).toString();
				}else{
					row[i-1] = "";
				}
			}
			allRows.add(row);
		}
		File csvFile = new File(fileName);
		FileUtils.forceMkdir(csvFile.getParentFile());
		if(csvFile.exists() && csvFile.isFile()){
			logger.info("File exist,delete first: "+fileName);
			FileUtils.deleteQuietly(csvFile);
		}
	    CSVWriter csvWriter= new CSVWriter(new FileWriter(fileName), '\t', CSVWriter.NO_QUOTE_CHARACTER);
	    csvWriter.writeAll(allRows);
        csvWriter.flush();
        csvWriter.close();
	}
	
	public static Connection getConnection(String driver, String url, String username, String password) throws Exception {
		Class.forName(driver);
		Connection connection = DriverManager.getConnection(url,username,password);
		return connection;
	}
	
	public static void close(Connection connection,Statement statement) {
		if(statement != null){
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(connection != null){
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args){
		Connection conn = null;
		Statement stm = null;
		try {
			conn = getConnection("com.mysql.jdbc.Driver",
					"jdbc:mysql://rds6a6iyi9p1l3v45c42.mysql.rds.aliyuncs.com:3306/promo?useUnicode=true&characterEncoding=utf8",
					"luck3_read","123456A");
			stm = conn.createStatement();
			//ResultSet rs = statement.executeQuery("select * from u_user limit 10");
			stm.executeQuery("select * from u_user limit 10 into outfile '/mnt/data/share/ingest/incoming/lbs/u_user_2015-12-22.txt.tmp'");
			/*while(rs.next()){
				System.out.println("-------------");
				System.out.println(rs.getString(1)+"\t"+rs.getString(2)+"\t"+rs.getString(3));
			}*/
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			close(conn,stm);
		}
		
	}
}
