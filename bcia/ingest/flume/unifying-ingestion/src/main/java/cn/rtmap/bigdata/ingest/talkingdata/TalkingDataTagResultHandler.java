package cn.rtmap.bigdata.ingest.talkingdata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import cn.rtmap.bigdata.ingest.utils.URLUtil;

public class TalkingDataTagResultHandler {
	static final Logger LOGGER = LoggerFactory.getLogger(TalkingDataTagResultHandler.class);

	private TalkingDataParam param;
	private String endpoint = "https://api.talkingdata.com/dmp/tags/v1";
	private ObjectMapper mapper;

	private static final long BATCH_SIZE = 100000;
	
	public TalkingDataTagResultHandler(TalkingDataParam param) {
		this.param = param;
		this.mapper = new ObjectMapper();
	}
	
	private String composeURL() {
		String url = String.format("%s?appkey=%s&token=%s&mac=%s", endpoint, param.getAppkey(), param.getToken(), param.getMac());

		LOGGER.info(url);
		return url;
	}
	
	public String getResult() {
		//String result;
		try {
			String result = URLUtil.doHttpsGet(composeURL());
			TalkingDataResult obj = mapper.readValue(result, TalkingDataResult.class);
			TalkingDataTagResult tagResult = obj.getResult();

			if (null == tagResult) {
				return null;
			}
			
			StringBuilder sb = new StringBuilder();
			String prefix = String.format("%s\t%s\t%s\t%s\t%s", param.getAppkey(), param.getToken(), param.getMac(), tagResult.getTdid(), tagResult.getSeq());

			TalkingDataTags tags = tagResult.getTags();
			if (null != tags.getAppinterest()) {
				for (TalkingDataDescriptor d : tags.getAppinterest()) {
					String msg = String.format("%s\t%s\t%s\t%s\t%s\n", prefix, "appinterest", d.getLabel(), d.getName(), d.getWeight());
					sb.append(msg);
				}
			}
			
			if (null != tags.getGameinterest()) {
				for (TalkingDataDescriptor d : tags.getGameinterest()) {
					String msg = String.format("%s\t%s\t%s\t%s\t%s\n", prefix, "gameinterest", d.getLabel(), d.getName(), d.getWeight());
					sb.append(msg);
				}
			}

			if (null != tags.getDemography()) {
				for (TalkingDataDescriptor d : tags.getDemography()) {
					String msg = String.format("%s\t%s\t%s\t%s\t%s\n", prefix, "demography", d.getLabel(), d.getName(), d.getWeight());
					sb.append(msg);
				}
			}
			
			if (null != tags.getDeviceinfo()) {
				for (TalkingDataDescriptor d : tags.getDeviceinfo()) {
					String msg = String.format("%s\t%s\t%s\t%s\t%s\n", prefix, "deviceinfo", d.getLabel(), d.getName(), d.getWeight());
					sb.append(msg);
				}
			}
			
			if (null != tags.getLocation()) {
				for (TalkingDataDescriptor d : tags.getLocation()) {
					String msg = String.format("%s\t%s\t%s\t%s\t%s\n", prefix, "location", d.getLabel(), d.getName(), d.getWeight());
					sb.append(msg);
				}
			}
			
			if (null != tags.getConsumption()) {
				for (TalkingDataDescriptor d : tags.getConsumption()) {
					String msg = String.format("%s\t%s\t%s\t%s\t%s\n", prefix, "consumption", d.getLabel(), d.getName(), d.getWeight());
					sb.append(msg);
				}
			}

			if (null != tags.getGamedepth()) {
				for (TalkingDataDescriptor d : tags.getGamedepth()) {
					String msg = String.format("%s\t%s\t%s\t%s\t%s\n", prefix, "gamedepth", d.getLabel(), d.getName(), d.getWeight());
					sb.append(msg);
				}
			}
			
			if (null != tags.getIndustry()) {
				for (TalkingDataDescriptor d : tags.getIndustry()) {
					String msg = String.format("%s\t%s\t%s\t%s\t%s\n", prefix, "industry", d.getLabel(), d.getName(), d.getWeight());
					sb.append(msg);
				}
			}

			return sb.toString();
		} catch (IOException e) {
			LOGGER.error("", e);
		}
		return null;
	}

	public static void main(String[] args) {
		TalkingDataParam p = new TalkingDataParam();
		p.setAppkey("56a5bd716d941ff451391d09");
		p.setToken("UOa3WhjEk3VpcA");
		//p.setMac("90:B6:86:C3:45:62");

		//LOGGER.info("aaa");
		
		String macAddrFile = null;
		String destDir = null;
		if (args.length > 1) {
			macAddrFile = args[0];
			destDir = args[1];
		}

		File file = new File(macAddrFile);
		if (!file.exists()) {
			LOGGER.error("file for mac address is not found " + macAddrFile);
			return;
		}

		File dir = new File(destDir);
		if (!dir.exists()) {
			LOGGER.error("dest directory for result doesn't exist " + destDir);
			return;
		}
		
		//FileReader rdr = new FileReader(file);
		//FileInputStream in = new FileInputStream(file);
		
		LOGGER.info(macAddrFile);
		LOGGER.info(destDir);
		
		
		
		long idx = 0;
		long count = 0;
		String line;
		BufferedReader buf;
		Map<String, FileWriter> map = new HashMap<String, FileWriter>();

		try {
			buf = new BufferedReader(new FileReader(file));
			while ((line = buf.readLine()) != null) {
				String[] toks = line.split("\t");
				if (toks.length >= 2) {
					String mac = toks[0];
					//String macAppearTimes = toks[1];
					LOGGER.info(mac);
					//p.setMac("90:B6:86:C3:45:62");
					p.setMac(mac);
					String result = getResult(p);
					if (null == result) {
						LOGGER.warn("failed to get result for mac: " + mac);
						continue;
					}

					if (null != result) {
						String key = String.format("tags_%s", (long) Math.floor(count / (BATCH_SIZE * 1.0)));
						String dstFullPath = String.format("%s/%s", destDir, key);
						FileWriter writer = null;
						
						if (!map.containsKey(key)) {
							writer = new FileWriter(dstFullPath);
							map.put(key, writer);
						} else {
							writer = map.get(key);
						}
						
						if (null != writer) {
							writer.write(result);
							writer.flush();
						}
						count++;
					}
					
				} else {
					LOGGER.error("invalid record " + line);
				}
				idx++;
				LOGGER.info("processed line " + idx + " successfully");
			}
		} catch (IOException e) {
			LOGGER.error("", e);
		} finally {
			for (String k : map.keySet()) {
				FileWriter w = map.get(k);
				if (null != w) {
					try {
						w.close();
					} catch (IOException e) {
						LOGGER.error("", e);
					}
				}
			}
		}

		//getResult(p);
		
		
		
		
	}

	private static String getResult(TalkingDataParam p) {	
		TalkingDataTagResultHandler t = new TalkingDataTagResultHandler(p);
		String res = null;
		try {
			res = t.getResult();
			Thread.sleep(10);
		} catch (InterruptedException e) {
			LOGGER.error("", e);
		}
		return res;
	}
}
