package cn.rtmap.bigdata.ingest.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.constant.DBConstants;
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
	
	/**
     * 压缩文件
     *
     * @param file
     * @param zipfile
     * @return
     * @throws Exception
     */
    public static boolean zipFile(String file, String zipfile,boolean deleteSrc) throws Exception {
        boolean bf = true;
        File srcfile = new File(file);
        if (!srcfile.exists()) {
        	logger.error("file to zip not exist : " + file);
            return true;
        }
        File destfile = new File(zipfile);
        if (destfile.exists()) {
        	logger.info("zip file have existed,delete first");
        	destfile.delete();
        }
        new File(zipfile).createNewFile();
        String fileName = srcfile.getName().replaceAll(CommonConstants.DEFAULT_TMP_EXTENSION, CommonConstants.DEFAULT_FILE_EXTENSION);
        FileOutputStream out = null;
        ZipOutputStream zipOut = null;
        FileInputStream in = null;
        try {
        	out = new FileOutputStream(zipfile);
            zipOut = new ZipOutputStream(out);
            in = new FileInputStream(file);
            ZipEntry entry = new ZipEntry(fileName);
            zipOut.putNextEntry(entry);
            // 向压缩文件中输出数据
            int nNumber = 0;
            byte[] buffer = new byte[4096];
            while ((nNumber = in.read(buffer)) != -1) {
                zipOut.write(buffer, 0, nNumber);
            }
        } catch (IOException e) {
        	bf = false;
            throw e;
        }finally{
        	if(in != null){in.close();}
        	if(zipOut != null){zipOut.close();}
        	if(out != null){out.close();}
        }
        if(deleteSrc){
        	srcfile.delete();
        }
        return bf;
    }
    /**
     * 压缩文件
     *
     * @param file
     * @param zipfile
     * @return
     * @throws Exception
     */
    public static boolean zipFile(File file, String zipfile) throws Exception {
        boolean bf = true;
        if (!file.exists()) {
        	logger.error("file to zip not exist : " + file.getName());
            return true;
        }
        File destfile = new File(zipfile);
        if (destfile.exists()) {
        	logger.info("zip file have existed,delete finish!");
        	destfile.delete();
        	destfile=null;
        }
        new File(zipfile).createNewFile();
        FileOutputStream out = null;
        ZipOutputStream zipOut = null;
        FileInputStream in = null;
        try {
        	out = new FileOutputStream(zipfile);
            zipOut = new ZipOutputStream(out);
            in = new FileInputStream(file);
            ZipEntry entry = new ZipEntry(file.getName());
            zipOut.putNextEntry(entry);
            // 向压缩文件中输出数据
            int nNumber = 0;
            byte[] buffer = new byte[4096];
            while ((nNumber = in.read(buffer)) != -1) {
                zipOut.write(buffer, 0, nNumber);
            }
        } catch (IOException e) {
        	bf = false;
            throw e;
        }finally{
        	IOUtils.closeQuietly(in);
        	IOUtils.closeQuietly(zipOut);
        	IOUtils.closeQuietly(out);
        }
        return bf;
    }
	public static boolean saveResultSetAsCSV(ResultSet rs, String fileName) throws Exception {
		if(rs == null || fileName == null){
			logger.error("save resultset as csv error: resultset or fileName is null");
			return false;
		}
		List<String[]> allRows = new ArrayList<String[]>();
		String[] row=null;
		ResultSetMetaData meta = rs.getMetaData();
		int num = meta.getColumnCount();
		while (rs.next()) {
			row = new String[num];
			for (int i = 1; i <= num; ++i) {
				if (rs.getObject(i) != null){
					//replace \t \r \n with special sign
					row[i-1] = rs.getObject(i).toString()
							.replaceAll(DBConstants.SIGN_TAB, DBConstants.REPLACE_TAB)
							.replaceAll(DBConstants.SIGN_RET_R, DBConstants.REPLACE_RET_R)
							.replaceAll(DBConstants.SIGN_RET_N, DBConstants.REPLACE_RET_N);
				}else{
					row[i-1] = "";
				}
			}
			allRows.add(row);
		}
		logger.info("resultset line count is "+allRows.size());
		if(allRows.size() == 0){
			return false;
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
        return true;
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
	
	/**
     * Get MD5 of one file:hex string
     */
    public static String getFileMD5(File file) {
        if (!file.exists() || !file.isFile()) {
            return null;
        }
        MessageDigest digest = null;
        FileInputStream in = null;
        byte buffer[] = new byte[1024];
        int len;
        try {
            digest = MessageDigest.getInstance("MD5");
            in = new FileInputStream(file);
            while ((len = in.read(buffer, 0, 1024)) != -1) {
                digest.update(buffer, 0, len);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally{
        	try {
				if(in != null){
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        BigInteger bigInt = new BigInteger(1, digest.digest());
        return bigInt.toString(16);
    }
    
    public static void createVerfFile(File file, String fileName, String verfFile) throws IOException{
    	String md5 = getFileMD5(file);
    	JSONObject json = new JSONObject();
    	json.put("filename", fileName);
    	json.put("md5sum", md5);
    	json.put("compress", "zip");
    	FileUtils.writeStringToFile(new File(verfFile), json.toString());
    }
    
	public static void main(String[] args){
		try {
			zipFile("C:\\Users\\zxw\\Desktop\\temp\\yarn-site.xml", "C:\\Users\\zxw\\Desktop\\temp\\yarn-site.zip",false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
