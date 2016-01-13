package cn.rtmap.bigdata.ingest.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.constant.RuianConstants;
import cn.rtmap.bigdata.ingest.utils.DateUtils;
import cn.rtmap.bigdata.ingest.utils.IngestUtil;

/**
 * Extractor data
 */
public class RuianGetExtractor implements Extractor {
	static final Logger logger = LoggerFactory.getLogger(RuianGetExtractor.class);
	String incoming = null;
	int beforeDays = 6;
	static StringBuffer detailLog = new StringBuffer();
	static boolean stat = false;
	String tmpFile = null;
			
	@Override
	public void init(Context ctx) {
		incoming = ctx.getString(CommonConstants.CONFIG_INCOMING_DIR);
		beforeDays = ctx.getInteger(RuianConstants.CONFIG_BEFORE_DAYS, 6);
	}
	
	@Override
	public boolean prepare() {
		return true;
	}
	
	@Override
	public Iterator<JsonElement<String, String>> getData() {
		stat=extract(null);
		logger.info(detailLog.toString());
		JsonElement<String, String> o = new JsonElement<String, String>();
		o.addHeader(CommonConstants.PROP_MAIL_SUBJECT, "ruian extract "+(stat?"success":"fail"));
		o.addHeader(CommonConstants.PROP_MAIL_CONTENT, detailLog.toString().replaceAll("\r\n", "<br>"));
		return Arrays.asList(o).iterator();
	}
	
	@SuppressWarnings("rawtypes")
	public boolean extract(String date){
		if(StringUtils.isBlank(date)){
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DATE, 0-beforeDays);
			try {
				date=DateUtils.formatDate(cal.getTime(), "yyyy-MM-dd");
			} catch (ParseException e) {
				detailLog.append(e.getLocalizedMessage());
				logger.error("parse date error: "+e.getLocalizedMessage(),e);
				return false;
			}
		}
		detailLog.append("\r\n").append("start ruian extract job: "+date);
		String fileName = "a_" + date.replaceAll("-", "") +  "_ruian"; 
		String filePath = incoming + "/" + date.replaceAll("-", "") + "/day/";
		tmpFile  = filePath+fileName+CommonConstants.DEFAULT_TMP_EXTENSION;
		String veriFile = filePath+fileName+CommonConstants.DEFAULT_VERF_EXTENSION;
		String zipFile  = filePath+fileName+CommonConstants.DEFAULT_ZIP_EXTENSION;
		List<String> list = new ArrayList<String>();
		
		for(int hour=0; hour<24; hour++){
			String hh = hour<10?"0"+hour:""+hour;
			String startTime = date + "T" + hh + ":00:00.000Z";
			String endTime = date + "T" + hh + ":59:59.000Z";
			String raDatas = "";
			InputStream is = null;
			InputStreamReader isr = null;
			BufferedReader br = null;
			try {
				URL url = new URL("https://sso.run.com:8443/exchanger/getrelatedid?startTime="+startTime+"&endTime="+endTime);
				HttpURLConnection con = (HttpURLConnection) url.openConnection();
				con.setRequestMethod("GET");
				con.setDoOutput(true);
				con.setDoInput(true);
				con.setConnectTimeout(30000);
				con.setReadTimeout(60000);
				if (con.getResponseCode() != 200) {
					detailLog.append("\r\n").append("hour "+hh+" fail, http response code "+con.getResponseCode());
					return false;
				}
				is = con.getInputStream();
				if (is == null) {
					detailLog.append("\r\n").append("hour "+hh+" fail, http inputstream null");
					return false;
				}
				isr = new InputStreamReader(is, "utf-8");
				br = new BufferedReader(isr);
				String line = "";
				while ((line = br.readLine()) != null) {
					raDatas += line;
				}
				if (StringUtils.isBlank(raDatas)){
					detailLog.append("\r\n").append("hour "+hh+" fail, http response body is blank");
					return false;
				}
				ObjectMapper mapper=new ObjectMapper();
				Map jsonObject = mapper.readValue(raDatas, Map.class);
				Integer status = (Integer) jsonObject.get("status");
				if ( 200 != status ){
					detailLog.append("\r\n").append("hour "+hh+" fail, ruian response status is "+status+",expect is 200");
					return false;
				}
				List jsonArrayData = (List) jsonObject.get("datas");
				if (jsonArrayData == null || jsonArrayData.size() == 0) {
					detailLog.append("\r\n").append("hour "+hh+" no data find");
					continue;
				}
				for (int i = 0; i < jsonArrayData.size(); i++) {
					String timestamp = date+" 00:00:00";
					String mac  = "null";
					String phone = "null";
					String idfa = "null";
					String imei = "null";
					String cityId = "null";
					String placeDesc = "null";
					Map jsonObjectData = (Map) jsonArrayData.get(i);
					for (Object k : jsonObjectData.keySet()){
						String key= (String) k;
						if (("phone").equals(key)) {
							phone = formatFieldValue((String) jsonObjectData.get("phone"));
						}
						if (("imei").equals(key)) {
							imei = formatFieldValue((String) jsonObjectData.get("imei"));
						}
						if (("idfa").equals(key)) {
							idfa = formatFieldValue((String) jsonObjectData.get("idfa"));
						}
						if (("mac").equals(key)) {
							mac = formatFieldValue((String) jsonObjectData.get("mac"));
						}
						if (("timestamp").equals(key)) {
							String time = (String) jsonObjectData.get("timestamp");
							if (StringUtils.isNotBlank(time)) {
								String arr[] = time.split("T");
								if(arr.length > 1){
									timestamp = arr[0] + " " + arr[1].substring(0,arr[1].length() - 5);
								}
							}
						}
						if (("cityid").equals(key)) {
							cityId = formatFieldValue((String) jsonObjectData.get("cityid"));
						}
						if (("place_desc").equals(key)) {
							placeDesc = formatFieldValue((String) jsonObjectData.get("place_desc"));
						}
					}
					if(!"null".equals(mac)){
						mac=mac.replaceAll(":", "").replaceAll("-", "").toUpperCase();
					}
					list.add(timestamp.trim()+"\t"+mac.trim()+"\t"+phone.trim()+"\t"+idfa.trim()+"\t"
					         +imei.trim()+"\t"+cityId.trim()+"\t"+placeDesc.trim());
				}
				File path=new File(filePath);
				if(!path.exists()){
					path.mkdirs();
				}
				FileUtils.writeLines(new File(tmpFile), "UTF-8", list, "\r\n", true);
				detailLog.append("\r\n").append("hour "+hh+" finish with lines "+list.size());
			} catch (Exception ex) {
				detailLog.append("\r\n").append("hour "+hh+" request error: " + ex.getLocalizedMessage());
				logger.error("hour "+hh+" request error: " + ex.getLocalizedMessage(),ex);
				return false;
			}finally{
				list.clear();
				IOUtils.closeQuietly(br);
				IOUtils.closeQuietly(isr);
				IOUtils.closeQuietly(is);
			}
		}
		detailLog.append("\r\n").append("http request finish!");
		try {
			IngestUtil.createVerfFile(new File(tmpFile), fileName+CommonConstants.DEFAULT_FILE_EXTENSION, veriFile);
			IngestUtil.zipFile(tmpFile,zipFile,true);
			detailLog.append("\r\n").append("create verify file and zip finish!");
		} catch (Exception e) {
			detailLog.append("\r\n").append("create verify file or zip file error: " + e.getLocalizedMessage());
			logger.error("create verify file or zip file error: " + e.getLocalizedMessage(),e);
			return false;
		}finally{
			detailLog.append("\r\n").append("extract job finish!");
		}
		return true;
	}
	
	private String formatFieldValue(String value){
		if("".equals(value) || "unknow".equals(value)){
			value = "null";
		}
		return value;
	}
	
	@Override
	public void cleanup() {
		File tmpDat = new File(tmpFile);
		if(tmpDat.exists()){
			tmpDat.delete();
		}
	}

	public static void main(String[] args){
		String reqDate=null;
		if(args != null && args.length>0){
			reqDate=args[0];
		}
		RuianGetExtractor extractor=new RuianGetExtractor();
		stat=extractor.extract(reqDate);
		extractor.cleanup();
		logger.info("ruian extract "+(stat?"success":"fail"));
		logger.info(detailLog.toString());
	}

}
