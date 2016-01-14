package cn.rtmap.flume.source.sftp;

//import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
//import java.io.File;
import java.io.FileOutputStream;
//import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.ConnectionMonitor;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3DirectoryEntry;
import ch.ethz.ssh2.SFTPv3FileAttributes;
import ch.ethz.ssh2.SFTPv3FileHandle;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SFTPOperator {
	private static final Logger LOG = LoggerFactory.getLogger(SFTPOperator.class);
	
	//private static String sshHostName = "r2s5";
	//private static String ROOT_DIR = "/root/electrocar";
	//private static String ROOT_DIR_BAK = "/root/electrocar-bak";
	//private static String FLAG_FILE = "start.txt";
	//private static String MD5_FLAG = "_MD5.txt";
	//private static String EXT_NAME = ".txt";
	//private static int BLOCK_SIZE = 20 * 1024;

	//private static String user = "root";
	//private static String passwd = "passw0rd";

	private Connection conn = null;
	private SFTPv3Client client = null;

	
	ChannelSftp sftp;
	Session sshSession;
	Channel channel;


	public SFTPOperator() throws JSchException {
		conn = new Connection(Constants.SFTP_HOSTNAME, Constants.SFTP_PORT);
	}


	private void initJSch() throws JSchException {
		sftp = null;
    	
    	sshSession = new JSch().getSession(Constants.SFTP_USER, Constants.SFTP_HOSTNAME, Constants.SFTP_PORT);
    	//System.out.println("Session created.");
    	sshSession.setPassword(Constants.SFTP_PASSWD);
    	Properties sshConfig = new Properties();
    	sshConfig.put("StrictHostKeyChecking", "no");
    	sshSession.setConfig(sshConfig);
    	sshSession.connect();

    	//System.out.println("Session connected.");
    	//System.out.println("Opening Channel.");
    	Channel channel = sshSession.openChannel("sftp");
    	channel.connect();
    	
    	sftp = (ChannelSftp) channel;
    	//System.out.println("Connected to " + "dn02" + ".");
	}
	
	
	void closeJSch() {
		sftp.exit();
    	sshSession.disconnect();
	}

    /*public static void main( String[] args ) throws IOException, JSchException, SftpException {
    	SFTPOperator ftp = new SFTPOperator();
    	try {
    		ftp.login();
    		List<HashMap> itemList = ftp.getItemList();
    		List<DataFile> dfs = ftp.getDataFileList(itemList);

    		List<String> dirList = new ArrayList<String>();
    		for (DataFile df : dfs) {
    			if (!dirList.contains(df.getFilePath())) {
    				dirList.add(df.getFilePath());
    				ftp.moveForBakcup(df.getFilePath(), ROOT_DIR_BAK);
    			}
    			df.getAllRows();
    		}
    	} finally {
    		ftp.logout();
    	}    
    }*/

    public void login() throws IOException {
    	conn.addConnectionMonitor(new ConnectionMonitor() {
			@Override
			public void connectionLost(Throwable reason) {
				reason.getStackTrace();
				throw new RuntimeException(reason);
			}
    	});
    	conn.connect();

    	boolean isAuthenticated = conn.authenticateWithPassword(Constants.SFTP_USER, Constants.SFTP_PASSWD);
    	if (!isAuthenticated) {
    		throw new IOException("Connect SSH Authentication failed.");
    	}

    	LOG.info("Connecting to " + Constants.SFTP_HOSTNAME + " successfully.");    	
    	client = new SFTPv3Client(conn);
    }

	public List<HashMap> getItemList() throws IOException {
    	client.setCharset("UTF-8");
    	List<HashMap> items = new ArrayList<HashMap>();
    	
    	List<SFTPv3DirectoryEntry> dateDirs = client.ls(Constants.SFTP_ROOT_DIR);
    	for (SFTPv3DirectoryEntry date : dateDirs) {
    		if (".".equals(date.filename) || "..".equals(date.filename)) {
    			continue;
    		}
    		if (!date.attributes.isDirectory()) {
    			continue;
    		}

        	List<SFTPv3DirectoryEntry> dirList = client.ls(Constants.SFTP_ROOT_DIR + "/" + date.filename);
        	for (SFTPv3DirectoryEntry entry : dirList) {
        		if (".".equals(entry.filename) || "..".equals(entry.filename)) {
        			continue;
        		}
        		if (!entry.attributes.isDirectory()) {
        			continue;
        		}
        		
        		String filePath = Constants.SFTP_ROOT_DIR + "/" + date.filename + "/" + entry.filename;
        		SFTPv3FileAttributes attr = client.stat(filePath);
        		if (attr.isDirectory()) {
        			List<SFTPv3DirectoryEntry> fileList = client.ls(filePath);
        			HashMap fileInfo = new HashMap();

        			boolean flagExist = false;
        			for (SFTPv3DirectoryEntry file : fileList) {
        				if (".".equals(file.filename) || "..".equals(file.filename)) {
        					continue;
        				}

        				if (Constants.SFTP_FLAG_FILE.equals(file.filename)) {
        					flagExist = true;
        				} else {
        					HashMap map = new HashMap();
        					String dataFile = filePath + "/" + file.filename;
        					LOG.info("scan file:" + dataFile);
        					fileInfo.put(dataFile, file.attributes.size);
        				}
        			}

        			if (flagExist) {
        				items.add(fileInfo);
        			}
        		}
        	}
    	}

    	return items;
	}

    public void logout() {
    	if (client != null)
    		client.close();

    	if (conn != null)
    		conn.close();
    }

    
    public List<DataFile> getDataFileList(List<HashMap> itemList) throws IOException, JSchException, SftpException {
    	List<DataFile> dataFileList = new ArrayList<DataFile>();
		String md5sum = "";

    	for (HashMap map : itemList) {
    		for (Object key : map.keySet()) {
    			String fileName = (String)key;
    			Long fileSize = (Long)map.get(key);
    			if (fileName.endsWith(Constants.SFTP_FLAG_MD5_FLAG)) {
    				String md5File = fileName;
    				List<byte[]> blocks = getRawContentV2(md5File, fileSize);
    				if (blocks != null && !blocks.isEmpty()) {
    					byte[] block = blocks.get(0);
    					String content = new String(block);
    					md5sum = content.split(" ")[0];
    					LOG.info("md5sum : " + md5sum);
    				}

    				String dataFileFullName = md5File.substring(0, md5File.indexOf(Constants.SFTP_FLAG_MD5_FLAG)) + Constants.SFTP_EXT_NAME;
    				blocks = getRawContentV2(dataFileFullName, (Long)map.get(dataFileFullName));

    				String[] tok = dataFileFullName.split("/");
    				String dataFileName = tok[tok.length - 1];
    				LOG.info(dataFileName);

    				String dataFilePath = dataFileFullName.substring(0, dataFileFullName.lastIndexOf(dataFileName));
    				LOG.info(dataFilePath);
    				
    				DataFile di = new DataFile();
    				di.setFileName(dataFileName);
    				di.setFilePath(dataFilePath);
    				di.setMd5Sum(md5sum);
    				di.setData(blocks);
    				dataFileList.add(di);
    			}
    		}
    	}

    	return dataFileList;
    }

    public void changeFileStatus(DataFile di, String status) throws IOException {
    	String oldFullName = di.getFilePath() + di.getFileName();
    	String newFullName = oldFullName + "." + status.toUpperCase();
    	client.mv(oldFullName, newFullName);    
    }

    public void backup(String filePath) throws IOException {
    	moveForBakcup(filePath, Constants.SFTP_ROOT_DIR_BAK);
    }

    private void moveForBakcup(String oldPath, String newPath) throws IOException {
    	String dstPath;
    	String[] toks = oldPath.split("/");
    	if (newPath.endsWith("/")) {
    		dstPath = String.format("%s%s", newPath, toks[toks.length - 1]);
    	} else {
    		dstPath = String.format("%s/%s", newPath, toks[toks.length - 1]);
    	}

    	// move the whole source folder to backup folder
    	client.mv(oldPath, dstPath);
    	LOG.info("moved " + oldPath + " to " + dstPath);
    }
    
    private List<byte[]> getRawContent(String filePath, long fileSize) throws IOException {    	
    	int readSize;
    	byte[] buffer;

    	List<byte[]> blocks = new ArrayList<byte[]>();
    	SFTPv3FileHandle fileHandle = null;

    	try {
    		long numread = 0;
    		long offset = 0;
    		fileHandle = client.openFileRW(filePath);
    		//fileHandle = client.openFile(filePath, 
    		//client.openFile(fileName, flags, attr)

    		/*if (fileSize <= BLOCK_SIZE) {
    			readSize = (int)fileSize;
    			buffer = new byte[readSize];
    			numread = client.read(fileHandle, offset, buffer, 0, readSize);
    			if (numread != -1) {
    				blocks.add(buffer);
    				LOG.info("file: " + filePath + ", read size in bytes: " + readSize);
    			}
    		} else {*/
    			OutputStream fstream = null;
    			String fileNamex = filePath.substring(filePath.lastIndexOf("/"), filePath.length()); 
    			fstream = new FileOutputStream(String.format("D:\\abc\\%s", fileNamex));
    			
    			//readSize = BLOCK_SIZE;
    			readSize = fileSize > Constants.SFTP_FILE_BLOCK_SIZE ? Constants.SFTP_FILE_BLOCK_SIZE : (int)fileSize;
    			buffer = new byte[readSize];
    			//offset = BLOCK_SIZE;
    			while (readSize > 0 && (numread = client.read(fileHandle, offset, buffer, 0, readSize)) != -1) {
    				offset += numread;
    				//blocks.add(bytes);
    				LOG.info("length:" + offset);
    				LOG.info("file: " + filePath + ", batch read in bytes: " + numread + ", block size:" + Constants.SFTP_FILE_BLOCK_SIZE);
    				fstream.write(buffer);

    				readSize = fileSize > (offset + Constants.SFTP_FILE_BLOCK_SIZE) ? Constants.SFTP_FILE_BLOCK_SIZE :  (int)(fileSize - offset);
    				buffer = new byte[readSize];
    			}
    			fstream.close();
    		//}
    	} finally {
    		if (fileHandle != null) {
    			client.closeFile(fileHandle);
    			fileHandle = null;
    		}
    	}

    	return blocks;
    }

    private List<byte[]> getRawContentV2(String filePath, long fileSize) throws IOException, JSchException, SftpException {    	
    	initJSch();
    	
    	String path = filePath.substring(0, filePath.lastIndexOf("/"));
    	//System.out.println(path);
    	LOG.info("file path: " + path);
    	sftp.cd(path);
    	
    	String fileName = filePath.substring(filePath.lastIndexOf("/") +1, filePath.length());
    	
    	//String dstPath = String.format("D:\\df4\\%s", fileNamex);
    	//File file=new File(dstPath);

    	//System.out.println("file:" + fileNamex);
    	ByteArrayOutputStream out = new ByteArrayOutputStream();
    	LOG.info("file name: " + fileName);
    	sftp.get(fileName, out);

    	byte[] bytes = out.toByteArray();
    	List<byte[]> blocks = new ArrayList<byte[]>();
    	blocks.add(bytes);

    	closeJSch();
    	return blocks;
    }

    /*private int readFile(String filePath, long offset, byte[] buf, int start, int len) throws IOException {
    	SFTPv3Client clnt = new SFTPv3Client(conn);
    	SFTPv3FileHandle handle = clnt.openFileRO(filePath);
    	int numread = clnt.read(handle, offset, buf, start, len);
    	clnt.closeFile(handle);
    	clnt.close();

    	return numread;
    }*/
}
