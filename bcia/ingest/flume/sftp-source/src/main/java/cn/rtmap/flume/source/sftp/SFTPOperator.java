package cn.rtmap.flume.source.sftp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.ConnectionMonitor;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3DirectoryEntry;
import ch.ethz.ssh2.SFTPv3FileAttributes;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SFTPOperator {
	private static final Logger LOG = LoggerFactory.getLogger(SFTPOperator.class);

	private Connection conn = null;
	private SFTPv3Client client = null;
	private final String dateFolderPrefix = "20";
	
	ChannelSftp sftp;
	Session sshSession;
	Channel channel;

	public SFTPOperator() throws JSchException {
		conn = new Connection(Constants.SFTP_HOSTNAME, Constants.SFTP_PORT);
	}


	private void initJSch() throws JSchException {
		sftp = null;
    	
    	sshSession = new JSch().getSession(Constants.SFTP_USER, Constants.SFTP_HOSTNAME, Constants.SFTP_PORT);
    	sshSession.setPassword(Constants.SFTP_PASSWD);
    	Properties sshConfig = new Properties();
    	sshConfig.put("StrictHostKeyChecking", "no");
    	sshSession.setConfig(sshConfig);
    	sshSession.connect();

    	Channel channel = sshSession.openChannel("sftp");
    	channel.connect();

    	sftp = (ChannelSftp) channel;
	}
	
	
	void closeJSch() {
		if (sftp != null) {
			sftp.exit();
		}
		if (sshSession != null) {
			sshSession.disconnect();
		}
	}


    public void login() throws IOException, JSchException {
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

    	LOG.debug("Connecting to " + Constants.SFTP_HOSTNAME + " successfully.");    	
    	client = new SFTPv3Client(conn);
    	
    	initJSch();
    }

	public List<HashMap> getItemList() throws IOException, SftpException {
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

    		int validCount = 0;
    		if (!date.filename.startsWith(dateFolderPrefix)) {
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
        					LOG.debug("scan file:" + dataFile);
        					fileInfo.put(dataFile, file.attributes.size);
        				}
        			}

        			if (flagExist) {
        				items.add(fileInfo);
        			}
        		}
        		validCount++;
        	}

        	if (validCount <= 0) {
        		String dateDir = Constants.SFTP_ROOT_DIR + "/" + date.filename;
        		sftp.rm(dateDir + "/*");
        		sftp.rmdir(dateDir);
        	}
    	}

    	return items;
	}

    public void logout() {
    	if (client != null)
    		client.close();

    	if (conn != null)
    		conn.close();
    	
    	closeJSch();
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
    					LOG.debug("md5sum : " + md5sum);
    				}

    				String dataFileFullName = md5File.substring(0, md5File.indexOf(Constants.SFTP_FLAG_MD5_FLAG)) + Constants.SFTP_EXT_NAME;
    				blocks = getRawContentV2(dataFileFullName, (Long)map.get(dataFileFullName));

    				String[] tok = dataFileFullName.split("/");
    				String dataFileName = tok[tok.length - 1];

    				String dataFilePath = dataFileFullName.substring(0, dataFileFullName.lastIndexOf(dataFileName));
    				
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

    private void moveForBakcup(String oldPath, String newPath) {
    	String dstPath;
    	String dstDateDir;
    	String[] toks = oldPath.split("/");
    	if (newPath.endsWith("/")) {
    		dstDateDir = String.format("%s%s", newPath, toks[toks.length - 2]);
    	} else {
    		dstDateDir = String.format("%s/%s", newPath, toks[toks.length - 2]);
    	}

    	dstPath = String.format("%s/%s", dstDateDir, toks[toks.length - 1]);
    	try {
			Log.info("creating remote dir:" + dstDateDir);
			try {
				sftp.mkdir(dstDateDir);
			} catch (Exception ex) {
				LOG.warn("seems dir " + dstDateDir + " already exists.");
			}
			sftp.chmod(0755, dstDateDir);
			client.mv(oldPath, dstPath);
			LOG.info("moved " + oldPath + " to " + dstPath);
		} catch (SftpException | IOException e) {
			LOG.error("move directory failed", e);
		}
    }

    private List<byte[]> getRawContentV2(String filePath, long fileSize) throws IOException, JSchException, SftpException {    	
    	String path = filePath.substring(0, filePath.lastIndexOf("/"));
    	LOG.debug("file path: " + path);
    	sftp.cd(path);

    	String fileName = filePath.substring(filePath.lastIndexOf("/") +1, filePath.length());
    	ByteArrayOutputStream out = new ByteArrayOutputStream();
    	LOG.debug("file name: " + fileName);
    	sftp.get(fileName, out);

    	byte[] bytes = out.toByteArray();
    	List<byte[]> blocks = new ArrayList<byte[]>();
    	blocks.add(bytes);
    	return blocks;
    }
}
