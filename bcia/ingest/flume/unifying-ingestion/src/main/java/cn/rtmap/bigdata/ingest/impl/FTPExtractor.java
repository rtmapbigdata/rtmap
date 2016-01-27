package cn.rtmap.bigdata.ingest.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.utils.Compressor;
import cn.rtmap.bigdata.ingest.utils.FTPManager;
import cn.rtmap.bigdata.ingest.utils.FileNameSelector;
import cn.rtmap.bigdata.ingest.utils.ZipUtil;

public class FTPExtractor implements Extractor {
	private static final Logger LOGGER = LoggerFactory.getLogger(FTPExtractor.class);

	private String workingDirectory;
	private String localWorkingDirectory;

	private int port;
	private String hostName;

	private String userName;
	private String passwd;
	
	FTPManager ftp;
	private String suffix;
	private String srcFrom;
	
	private String prevDate;
	private String initDate;
	private List<String> dates;
	private String currDate;
	private String ctrlFile;
	private int batchLines;
	private String dataFileSuffix;
	private String unitCode;

	private ChannelProcessor channel;
	
	public FTPExtractor(ChannelProcessor channel) {
		this.channel = channel;
	}
	
	@Override
	public void init(Context ctx) {
		workingDirectory = ctx.getString("ftp.working.dir");
		localWorkingDirectory = ctx.getString("local.working.dir");
		hostName = ctx.getString("ftp.host.name");
		port = ctx.getInteger("ftp.port");
		userName = ctx.getString("ftp.user");
		passwd = ctx.getString("ftp.password");
		suffix = ctx.getString("ftp.file.suffix");

		ftp = new FTPManager(hostName, port, userName, passwd);
		ftp.setWorkingDirectory(workingDirectory);
		ftp.setLocalWorkingDir(localWorkingDirectory);
		
		dates = new ArrayList<String>();
		initDate = ctx.getString("ftp.date.initial");
		ctrlFile = ctx.getString("ftp.ctrl.file");
		srcFrom = ctx.getString("data.from");
		batchLines = ctx.getInteger("data.batch.lines");
		dataFileSuffix = ctx.getString("data.file.suffix");
		unitCode = ctx.getString("data.unit.code");
	}

	@Override
	public boolean prepare() {
		currDate = null;
		dates.clear();

		String root = workingDirectory;
		try {
			ftp.connect();
			prevDate = getsWaterMark(initDate);

			List<FTPFile> list = ftp.listElements(root);
			if (!list.isEmpty()) {
				for (FTPFile element : list) {
					String elementName = ftp.getObjectName(element);
					
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					Date d = sdf.parse(elementName);
				
					String today = sdf.format(new Date());
					if (!(d.after(sdf.parse(prevDate)) && d.before(sdf.parse(today)))) {
						continue;
					}
					
					dates.add(elementName);
					Collections.sort(dates);
					currDate = dates.get(0);
				}
			}
		} catch (IOException | ParseException e) {
			LOGGER.error("", e);
		} finally {
			ftp.disconnect();
		}

		return (currDate != null);
	}

	@Override
	public Iterator<JsonElement<String, String>> getData() {
		String root = workingDirectory;
		String dir = String.format("%s/%s", root, currDate);
		
		List<FTPFile> list;
		try {
			ftp.connect();

			File localDir = null;
			list = ftp.listElements(dir);
			for (FTPFile element : list) {
				String elementName = ftp.getObjectName(element);
				if (elementName.endsWith(suffix) && ftp.isFile(element)) {
					String fileName = String.format("%s/%s", dir, elementName);
					LOGGER.info(fileName);
					
					String localDirName = ftp.getLocalWorkingDir() + "/" + currDate;
					localDir = new File(localDirName);
					if (!localDir.exists()) {
						localDir.mkdirs();
					}

					String localFileName = localDirName + "/" + elementName;
					String localFileNameTmp = localFileName + ".tmp";
					LOGGER.info(localFileName);

					if (!new File(localFileName).exists()) {
						InputStream in = ftp.getInputStream(fileName, FTP.BINARY_FILE_TYPE);
						boolean result = ftp.readStream(in, localFileNameTmp);
						if (result) {
							File tmp = new File(localFileNameTmp);
							tmp.renameTo(new File(localFileName));
						}
					}

					if (new File(localFileName).exists()) {
						ZipUtil.unzip(localFileName, localDirName, false);
					}
					break;
				}
			}

			processFiles(localDir);
		} catch (IOException e) {
			LOGGER.error("", e);
			return null;
		} finally {
			ftp.disconnect();
		}

		return null;
	}

	@Override
	public void cleanup() {
		prevDate = currDate;
		updateWaterMark(prevDate);
	}

	private void processFiles(File dir) {
		if (dir != null) {
			File[] files = dir.listFiles(new FileNameSelector(dataFileSuffix));
			Arrays.sort(files, new Comparator<File>() {
				public int compare(File f1, File f2) {
					String n1 = f1.getName();
					String n2 = f2.getName();

					int idx1 = n1.lastIndexOf(".");
					int idx2 = n2.lastIndexOf(".");

					String sub1 = n1.substring(19, idx1);
					String sub2 = n2.substring(19, idx2);

					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd H_m_s");
					Date d1 = null;
					Date d2 = null;
					try {
						d1 = sdf.parse(sub1);
						d2 = sdf.parse(sub2);
					} catch (ParseException e) {
						LOGGER.error("parse date error", e);
					}

					return d1.compareTo(d2);
				}
			});

			for (File dataFile : files) {
				if (dataFile.isFile()) {
					processFile(dataFile.getAbsolutePath(), currDate);
				}
			}
			deleteDirectory(dir);
		}
	}
	
	private void processFile(String fileName, String date) {
		long rowId = 0;

		String line;
		String buildId = new File(fileName).getName().split("_")[0];
		LOGGER.info("BUILDID:" + buildId);
		JsonElement<String, String> o;
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			StringBuffer buf = new StringBuffer();
			while ((line = br.readLine()) != null) {
				++rowId;
				String content = String.format("%s\n", line);
				buf.append(content);

				if (rowId % batchLines == 0) {
					o = processBlock(buildId, date, buf.toString().getBytes());
					buildAndProcessEvent(o);
					buf.setLength(0);
				}
			}

			if (buf.length() > 0) {
				o = processBlock(buildId, date, buf.toString().getBytes());
				buildAndProcessEvent(o);
			}
		} catch (IOException e) {
			LOGGER.error("Read file content failed", e);
		}
	}
	
	private JsonElement<String, String> processBlock(String buildid, String date, byte[] data) {
		JsonElement<String, String> o = new JsonElement<String, String>();
		byte[] content = Compressor.compress(data);
		o.setBody(content);

		o.addHeader(HeaderConstants.DEF_MODE, HeaderConstants.VAL_MODE_DATA);
		o.addHeader(HeaderConstants.DEF_FROM, srcFrom);
		o.addHeader(HeaderConstants.DEF_UNIT_CODE, unitCode);

		String yyyymmdd = date.replace("-", "");
		o.addHeader(HeaderConstants.DEF_PROCESS_MONTH, yyyymmdd.substring(0, "YYYYMM".length()));
		o.addHeader(HeaderConstants.DEF_PROCESS_DATE, yyyymmdd);
		o.addHeader(HeaderConstants.DEF_FILENAME, String.format("i_%s_%s", yyyymmdd, buildid));
		o.addHeader(HeaderConstants.DEF_COMPRESS, HeaderConstants.VAL_COMPRESS_GZIP);
		
		return o;
	}
	
	private void updateWaterMark(String waterMark) {
		Writer writer = null;
		File file = new File(ctrlFile);

		try {
			writer = new FileWriter(file, false);
			String content = String.format("%s:%s\t%s\t%s\n", hostName, port, userName, waterMark);
			writer.write(content);
			writer.close();
		} catch (IOException e) {
			LOGGER.error("Error writing water mark to control file!!!", e);
		}
	}

	private String getsWaterMark(String defaultWaterMark) {
		File file = new File(ctrlFile);
		String hostAndPort = String.format("%s:%s", hostName, port);

		if (!new File(ctrlFile).exists()) {
			LOGGER.info("Control file not created, using start value from config file");
			return defaultWaterMark;
		}
		else{
			try {
				FileReader reader = new FileReader(file);
				char[] chars = new char[(int) file.length()];
				reader.read(chars);
				String[] statusInfo = new String(chars).split("\t");
				if (statusInfo[0].equals(hostAndPort) && statusInfo[1].equals(userName)) {
					reader.close();
					LOGGER.info(ctrlFile + " correctly formed");           
					return statusInfo[2].replaceAll("\\r\\n|\\r|\\n", "");
				}
				else{
					LOGGER.warn(ctrlFile + " corrupt!!! Deleting it.");
					reader.close();
					deleteControlFile();
					return defaultWaterMark;
				}
			} catch (IOException e) {
				LOGGER.error("Corrupt watermark value in file!!! Deleting it.", e);
				deleteControlFile();
				return defaultWaterMark;
			}
		}
	}

	private void deleteControlFile() {
		File file = new File(ctrlFile);
		if (file.delete()){
			LOGGER.info("Deleted control file: {}",file.getAbsolutePath());
		}else{
			LOGGER.warn("Error control file: {}",file.getAbsolutePath());
		}
	}

	private boolean deleteDirectory(File dir) {
		if (dir.isDirectory()) {
			File[] children = dir.listFiles();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDirectory(children[i]);
				if (!success) {
					return false;
				}
			}
		}
		// either file or an empty directory
		LOGGER.info("removing file or directory : " + dir.getName());
		return dir.delete();
	}
	
	private void buildAndProcessEvent(JsonElement<String, String> o) {
		byte[] body = o.getBody();
		Map<String, String> headers = o.getHeaders();

		if (channel != null) {
			channel.processEvent(EventBuilder.withBody(body, headers));
		}
	}

	public static void main(String[] args) {
		Context ctx = new Context();
		ctx.put("ftp.working.dir", "/now");
		ctx.put("local.working.dir", "c:/agent/test");
		ctx.put("ftp.host.name", "114.251.74.172");
		ctx.put("ftp.port", "22222");
		ctx.put("ftp.user", "ftpuser");
		ctx.put("ftp.password", "joycity1788");
		ctx.put("ftp.file.suffix", ".zip");
		
		ctx.put("ftp.date.initial", "2016-01-20");
		ctx.put("ftp.ctrl.file", "c:/agent/test/ftp-nps.ctrl");
		
		ctx.put("data.from", "lbs");
		ctx.put("data.batch.lines", "10240");
		ctx.put("data.file.suffix", "csv");
		
		ctx.put("data.file.suffix", "csv");
		ctx.put("data.unit.code", "nps");

		Extractor ex = new FTPExtractor(null);
		ex.init(ctx);

		while (ex.prepare()) {
			ex.getData();
			ex.cleanup();
		}
	}
}
