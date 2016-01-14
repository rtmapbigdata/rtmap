package cn.rtmap.bigdata.ingest.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.constant.CommonConstants;

public class IngestUtil {
	private static final Logger logger = LoggerFactory.getLogger(IngestUtil.class);
	
	/**
     * get file md5 as hex string
     */
    public static String getFileMD5(File file) throws Exception {
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
        }finally{
			if (in != null) {
				in.close();
			}
        }
        BigInteger bigInt = new BigInteger(1, digest.digest());
        return bigInt.toString(16);
    }
    
    /**
     * create verify file
     * @param file
     * @param fileName
     * @param verfFile
     * @throws Exception
     */
    public static void createVerfFile(File file, String fileName, String verfFile) throws Exception{
    	String md5 = getFileMD5(file);
    	JSONObject json = new JSONObject();
    	json.put("filename", fileName);
    	json.put("md5sum", md5);
    	json.put("compress", "zip");
    	FileUtils.writeStringToFile(new File(verfFile), json.toString());
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
        	srcfile.deleteOnExit();
        }
        return bf;
    }
}
