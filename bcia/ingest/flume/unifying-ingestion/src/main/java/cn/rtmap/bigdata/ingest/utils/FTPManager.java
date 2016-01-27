package cn.rtmap.bigdata.ingest.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class FTPManager {
	 private static final Logger LOGGER = LoggerFactory.getLogger(FTPManager.class);
	 private FTPClient ftpClient;
	 
	 private int port;
	 private String server;
	 private String user;
	 private String passwd;
	 private String workingDir;
	 private String localWorkingDir;
	 
	 public FTPManager(String server, int port, String user, String passwd) {
		 this.server = server;
		 this.port = port;
		 this.user = user;
		 this.passwd = passwd;
		 ftpClient = new FTPClient();
	 }
	 
	 public boolean connect() {
		 try {
			 ftpClient.connect(server, port);
			 int replyCode = ftpClient.getReplyCode();
			 if (!FTPReply.isPositiveCompletion(replyCode)) {
				 ftpClient.disconnect();
				 LOGGER.error("Connect Failed due to FTP, server refused connection.");
				 return false;
			 }

			 if (!ftpClient.login(user, passwd)) {
				 LOGGER.error("Could not login to the server");
			 }

			 LOGGER.debug("connected successfully");
			 ftpClient.enterLocalPassiveMode();
			 ftpClient.setControlKeepAliveTimeout(300);
//			 if (getWorkingDirectory() != null) {
//				 ftpClient.changeWorkingDirectory(getWorkingDirectory());
//			 }
			 //ftpClient.setBufferSize(getBufferSize());
		 } catch (IOException e) {
			 LOGGER.error("", e);
			 return false;
		 }
		 return true;
	 }
	 
	 public void disconnect() {
		 try {
			 if (ftpClient != null) {
				 ftpClient.logout();
				 ftpClient.disconnect();
				 LOGGER.debug("disconnected successfully");
			 }
		 } catch (IOException e) {
			 LOGGER.error("failed to disconnect", e);
		 }
	 }
	 
	 public void changeToDirectory(String dir) throws IOException {        
		 ftpClient.changeWorkingDirectory(dir);        
	 }
	 
	 public List<FTPFile> listElements(String dir) throws IOException {
		 List<FTPFile> list = new ArrayList<>();
		 FTPFile[] subFiles = ftpClient.listFiles(dir);
		 list = Arrays.asList(subFiles);
		 return list;
	 }

	 public InputStream getInputStream(String file, int mode) throws IOException {
		 InputStream inputStream = null;
		 ftpClient.setFileType(mode);
		 inputStream = ftpClient.retrieveFileStream(file);

		 return inputStream;
	 }

	 public String getObjectName(FTPFile file) {
		 return file.getName();
	 }

	 public boolean isDirectory(FTPFile file) {
		 return file.isDirectory();
	 }

	 public boolean isFile(FTPFile file) {
		 return file.isFile();
	 }

	 public boolean particularCommand() {
		 boolean success = true;
		 try {
			 success = ftpClient.completePendingCommand();
		 } catch (IOException e) {
			 LOGGER.error("Error on command completePendingCommand of FTPClient", e);
		 }
		 return success;
	 }

	 public long getObjectSize(FTPFile file) {
		 return file.getSize();
	 }
	 
	 public String getWorkingDirectory() {
		 return workingDir;
	 }
	 
	 public boolean isLink(FTPFile file) {
		 return file.isSymbolicLink();
	 }
	 
	 public String getLink(FTPFile file) {
		 return file.getLink();
	 }
	 
	 public String getDirectoryserver() throws IOException {
		 String printWorkingDirectory = "";
		 printWorkingDirectory = ftpClient.printWorkingDirectory();
		 return printWorkingDirectory;
	 }
	 
	 public void setFileType(int fileType) throws IOException {
		 ftpClient.setFileType(fileType);
	 }
	 
	 public void setWorkingDirectory(String dir) {
		 this.workingDir = dir;
	 }
	 
	 public int getBufferSize() {
		 return 1024;
	 }
	 
	 public void setBufferSize() {
		 
	 }
	 
	 public boolean isFlushLines() {
		 return true;
	 }
	 
	 public int getChunkSize() {
		 return 1 * 1024 * 1024 * 10;
	 }
	 
	 public boolean readStream(InputStream inputStream, String fileName) throws IOException {
		 if (inputStream == null) {
			 return false;
		 }
		 FileOutputStream fos = null;
		 boolean successRead = true;
		 try {
			 int chunkSize = getChunkSize();
			 byte[] bytesArray = new byte[chunkSize];
			 int bytesRead = -1;
			 fos = new FileOutputStream (new File(fileName)); 
			 while ((bytesRead = inputStream.read(bytesArray)) != -1) {
				 try (ByteArrayOutputStream baostream = new ByteArrayOutputStream(chunkSize)) {
					 baostream.write(bytesArray, 0, bytesRead);
					 byte[] data = baostream.toByteArray();
					 fos.write(data);
				 }
				 fos.flush();
			 }
		 } catch (IOException e) {
			 LOGGER.error("on readStream", e);
			 successRead = false;

		 } finally {
			 if (fos != null) {
				 fos.close();
			 }
			 if (inputStream != null) {
				 inputStream.close();
			 }
		 }
		 return successRead;
	 }
	 
	 public static void main(String[] args) throws IOException {
		 FTPManager mgr = new FTPManager("114.251.74.172", 22222, "ftpuser", "joycity1788");
		 //mgr.setWorkingDirectory("c:/agent/test");
		 mgr.setLocalWorkingDir("c:/agent/test");
		 
		 mgr.connect();
		 String rootDir = "/now";
		 List<FTPFile> list = mgr.listElements(rootDir);
		 if (!list.isEmpty()) {
			 for (FTPFile elem : list) {
				 String elementName = mgr.getObjectName(elem);
				 if (elementName.equals(".") || elementName.equals("..")) {
					 continue;
				 }

				 if (mgr.isDirectory(elem)) {
					 String midDir = String.format("%s/%s", rootDir, elementName);
					 List<FTPFile> list2 = mgr.listElements(midDir);
					 for (FTPFile f : list2) {
						 if (mgr.isFile(f) && mgr.getObjectSize(f) > 0 && mgr.getObjectName(f).endsWith("zip")) {
							 LOGGER.debug(mgr.getObjectName(f));
							 String fileName = String.format("%s/%s",midDir, mgr.getObjectName(f));
							 InputStream inputStream = mgr.getInputStream(fileName, FTP.BINARY_FILE_TYPE);
							 String localFileName = mgr.getLocalWorkingDir() + "/" + mgr.getObjectName(f);
							 String localFileNameTmp = localFileName + ".tmp";
							 boolean result = mgr.readStream(inputStream, localFileNameTmp);
							 if (result) {
								 File tmpFile = new File(localFileNameTmp);
								 tmpFile.renameTo(new File(localFileName));
							 }
							 break;
						 }
					 }
				 }
				 break;
			 }
		 }
		 mgr.disconnect();
	 }

	public String getLocalWorkingDir() {
		return localWorkingDir;
	}

	public void setLocalWorkingDir(String localWorkingDir) {
		this.localWorkingDir = localWorkingDir;
	}
}
