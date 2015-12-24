package cn.rtmap.bigdata.ingest.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.flume.Context;

import cn.rtmap.bigdata.ingest.impl.HeaderConstants;
import cn.rtmap.bigdata.ingest.source.FileSourceConfigurationConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileManager {
	private static final Logger logger = LoggerFactory.getLogger(FileManager.class);

	private String incomingDir;
	//private String outgoingDir;
	private String bakDir;
	private String verfExt;

	private String ruleFile;
	private String compressFile;
	private String dataFile;
	private String fileDate;
	private String from;

	Queue<String> queue;
	static final int BUFFER = 2048;

	public FileManager(Context ctx) {

		incomingDir = ctx.getString(FileSourceConfigurationConstants.CONFIG_INCOMING_DIR);
		//outgoingDir = ctx.getString(FileSourceConfigurationConstants.CONFIG_OUTGOING_DIR);
		bakDir = ctx.getString(FileSourceConfigurationConstants.CONFIG_BACKUP_DIR);
		verfExt = ctx.getString(FileSourceConfigurationConstants.CONFIG_VERF_EXTENSION);
		from = ctx.getString(FileSourceConfigurationConstants.CONFIG_DATA_FROM);
		//fileSource = ctx.getString(FileSourceConfigurationConstants);

/*		incomingDir = "d:\\dev2\\ingest\\incoming\\lbs";
		//outgoingDir = "d:\\dev2\\ingest\\outgoing\\lbs";
		bakDir = "d:\\dev2\\ingest\\bak\\lbs";
		verfExt = ".verf";
*/
		queue = new Queue<String>();
		getFileList();
	}

	private void getFileList() {
		ListFilesUtil util = new ListFilesUtil();
		util.listFilesAndFilesSubDirectories(incomingDir, queue, verfExt);
	}

	public int getFileCount() {
		return queue.count();
	}

	public String nextFile() {
		ruleFile = queue.removeLast();
		if (ruleFile != null) {
			compressFile = ruleFile.replaceFirst(verfExt, ".zip");
			dataFile = decompress(compressFile);
			logger.info("picked up file: " + dataFile);
			return dataFile;
		} else {
			return null;
		}
	}

	private String decompress(String compressFile) {
		BufferedOutputStream dest = null;
		BufferedInputStream is = null;
		ZipEntry entry;
		String destFile = null; 

		try {
			ZipFile zipfile = new ZipFile(compressFile);
			Enumeration e = zipfile.entries();
			while (e.hasMoreElements()) {
				entry = (ZipEntry) e.nextElement();
				System.out.println("Extracting: " + entry);
				is = new BufferedInputStream(zipfile.getInputStream(entry));
				int count;
				byte data[] = new byte[BUFFER];
				String dir = compressFile.substring(0, compressFile.lastIndexOf(File.separator));
				destFile = String.format("%s%s%s", dir, File.separator, entry.getName());
				FileOutputStream fos = new FileOutputStream(destFile);
				dest = new BufferedOutputStream(fos, BUFFER);
				while ((count = is.read(data, 0, BUFFER)) != -1) {
					dest.write(data, 0, count);
				}
				dest.flush();
				dest.close();
				is.close();
			}
			zipfile.close();
			return destFile;
		} catch (Exception e) {
			logger.error("Decompress file failed", e);
			return null;
		}
	}

	public Map<String, String> getFileHeader() {
		Map<String, String> map = new HashMap<String, String>();
		String fileName = dataFile.substring(dataFile.lastIndexOf(File.separator) + 1, dataFile.length());
		String[] toks = fileName.split("_");

		if (toks.length >= 3) {
			fileDate = toks[1];
			map.put(HeaderConstants.DEF_MODE, HeaderConstants.VAL_MODE_DATA);
			map.put(HeaderConstants.DEF_FROM, from);
			map.put(HeaderConstants.DEF_UNIT_CODE, toks[2].split("\\.")[0]);
			// YYYYMMDD
			map.put(HeaderConstants.DEF_PROCESS_DATE, fileDate);
			// YYYYMM
			map.put(HeaderConstants.DEF_PROCESS_MONTH, fileDate.substring(0, "YYYYMM".length()));
			map.put(HeaderConstants.DEF_FILENAME, fileName.substring(0, fileName.lastIndexOf(".")));
		}
		return map;
	}

	public void backupFile() {
		String srcDir = String.format("%s%s%s%s%s", incomingDir, File.separator, fileDate, File.separator, "day");
        String dstDir = String.format("%s%s%s%s%s", bakDir, File.separator, fileDate, File.separator, "day");
        boolean result = false;

		File source = new File(srcDir);
		if (!source.exists()) {
			logger.error("File or directory does not exist: " + srcDir);
			return;
		}

		File destination = new File(dstDir);
		if (!destination.exists()) {
			result = destination.mkdirs();
		}

		File srcFile = new File(ruleFile);
		File dstFile = new File(String.format("%s%s%s", dstDir, File.separator, ruleFile.substring(ruleFile.lastIndexOf(File.separator), ruleFile.length())));
		if (srcFile.exists()) {
			result = srcFile.renameTo(dstFile);
		}

		srcFile = new File(compressFile);
		dstFile = new File(String.format("%s%s%s", dstDir, File.separator, compressFile.substring(compressFile.lastIndexOf(File.separator), compressFile.length())));
		if (srcFile.exists()) {
			result = srcFile.renameTo(dstFile);
		}

		srcFile = new File(dataFile);
		result = srcFile.delete();

		if (source.isDirectory()) {
			if (source.list().length <= 0)
				source.delete();
		}

		String part = String.format("%s%s%s", incomingDir, File.separator, fileDate);
		File partDir = new File(part);
		if (partDir.isDirectory()) {
			if (partDir.list().length <= 0) {
				result = partDir.delete();
				if (!result) {
					logger.error("delete directory failed: " + part);
				}
			}
		}

		//System.out.println(result);
	}

/*
	public static void main(String[] args) {
		FileManager fmgr = new FileManager();
		logger.info("File count:" + fmgr.getFileCount());
		logger.info("Last data file:" + fmgr.nextFile());
		fmgr.getFileHeader();
		fmgr.backupFile();
	}
*/
}
