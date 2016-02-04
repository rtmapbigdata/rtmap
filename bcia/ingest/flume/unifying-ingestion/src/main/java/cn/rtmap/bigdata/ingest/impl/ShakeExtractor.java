package cn.rtmap.bigdata.ingest.impl;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.utils.SQLOperator;
import cn.rtmap.bigdata.ingest.wx.shake.PageDescription;
import cn.rtmap.bigdata.ingest.wx.shake.PageDescriptionHandler;
import cn.rtmap.bigdata.ingest.wx.shake.PageStatisticsHandler;
import cn.rtmap.bigdata.ingest.wx.shake.PageStatisticsPacket;
import cn.rtmap.bigdata.ingest.wx.shake.PageStatisticsParams;
import cn.rtmap.bigdata.ingest.wx.shake.PageStatisticsReport;
import cn.rtmap.bigdata.ingest.wx.shake.TokenManager;

public class ShakeExtractor implements Extractor {
	private static final Logger LOGGER = LoggerFactory.getLogger(ShakeExtractor.class);
	private static final String UNIT_CODE_NAME = "shakepage";

	private String shakeStatisURL;
	private String shakeTokenURL;
	private String sql;
	private String dataFrom;

	private TokenManager tokmgr;
	private SQLOperator sqlOperator;
	private String token = null;
	private String ctrlFile;
	private String waterMark;
	private String prevWaterMark;
	private String initDate;
	private String untilDate;
	
	private SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
	public void init(Context ctx) {
		dataFrom = ctx.getString("data.from");
		shakeStatisURL = ctx.getString("shake.statis.url");

		shakeTokenURL = ctx.getString("shake.token.url");
		tokmgr = new TokenManager(shakeTokenURL);

		String driver = ctx.getString("sql.driver");
		String jdbcUrl = ctx.getString("sql.jdbc.url");
		String user = ctx.getString("sql.user");
		String passwd = ctx.getString("sql.passwd");
		sqlOperator = new SQLOperator(driver, jdbcUrl, user, passwd);

		sql = ctx.getString("sql.pageid.query");
		ctrlFile = ctx.getString("shake.ctrl.file");
		initDate = ctx.getString("shake.extract.init.date");
		untilDate = ctx.getString("shake.extract.until.date", formater.format(new Date()));
	}

	@Override
	public boolean prepare() {
		try {
			token = tokmgr.getToken();
			prevWaterMark = getWaterMark(initDate);
			
			Date d1 = formater.parse(prevWaterMark);
			Calendar cal = Calendar.getInstance();
			
			cal.setTime(d1);
			cal.add(Calendar.DATE, 1);
			d1 = cal.getTime();
			
			//Date d2 = new Date();
			//untilDate = formater.format(d2);
			if (d1.before(formater.parse(untilDate)) || d1.equals(formater.parse(untilDate))) {
				waterMark = formater.format(d1);
				LOGGER.info("waterMark:" + waterMark);
				LOGGER.info("prevWaterMark:" + prevWaterMark);
				return true;
			} else {
				return false;
			}
		} catch (IOException | ParseException e) {
			LOGGER.error("", e);
		}
		return false;
	}

	@Override
	public Iterator<JsonElement<String, String>> getData() {
		PageDescriptionHandler pdh = new PageDescriptionHandler(sqlOperator, sql);
		Iterator<PageDescription> it = pdh.getPageDescription();

		List<JsonElement<String, String>> elems = new ArrayList<JsonElement<String, String>>();
		StringBuffer sb = new StringBuffer();

		int cnt = 0;
		String url = String.format("%s%s", shakeStatisURL, token);

		while (it.hasNext()) {
			PageDescription pd = it.next();
			PageStatisticsParams p = new PageStatisticsParams();
			p.setPage_id(pd.getId());
			p.setBegin_date(dateStr2Sec(getYesterdayStr()));
			p.setEnd_date(dateStr2Sec(getTodayStr()));
			
//			try {
//				p.setBegin_date(formater.parse(prevWaterMark).getTime() / 1000);
//				p.setEnd_date(formater.parse(waterMark).getTime() / 1000);
//			} catch (ParseException e1) {
//				LOGGER.error("", e1);
//			}
			
			PageStatisticsHandler h = new PageStatisticsHandler(url, p);
			try {
				PageStatisticsPacket pack = h.getPacket();
				PageStatisticsReport report = new PageStatisticsReport(pd, pack);
				String content = report.toString().trim();
				if (!content.isEmpty()) {
					sb.append(String.format("%s\n", report.toString()));
					cnt++;					
				}
			} catch (IOException e) {
				LOGGER.error("Retrieve page statistics failed, " + p.toString(), e);
				//e.printStackTrace();
			}
		}

		if (cnt > 0) {
			JsonElement<String, String> je = new JsonElement<String, String>();
			je.addHeader(HeaderConstants.DEF_MODE, HeaderConstants.VAL_MODE_DATA);
			je.addHeader(HeaderConstants.DEF_FROM, dataFrom);
			je.addHeader(HeaderConstants.DEF_UNIT_CODE, UNIT_CODE_NAME);

			//String yyyymmdd = getYesterdayStr().substring(0, 10).replace("-", "");
			String yyyymmdd = prevWaterMark.replace("-", "");
			String yyyymm = yyyymmdd.substring(0, 6);

			je.addHeader(HeaderConstants.DEF_PROCESS_DATE, yyyymmdd);
			je.addHeader(HeaderConstants.DEF_PROCESS_MONTH, yyyymm);
			je.addHeader(HeaderConstants.DEF_FILENAME, String.format("a_%s_%s", yyyymmdd, UNIT_CODE_NAME));
			je.setBody(sb.toString().getBytes());

			//LOGGER.info(sb.toString());
			elems.add(je);
		}
		return elems.iterator();
	}

	@Override
	public void cleanup() {
		updateWaterMark(waterMark);
		prevWaterMark = waterMark;
	}

	private String getYesterdayStr() {
//		Calendar cal = Calendar.getInstance();
//		cal.add(Calendar.DATE, -1);
		
		return String.format("%s 00:00:00", prevWaterMark);
	}
	
	private String getTodayStr() {
//		Calendar cal = Calendar.getInstance();
//		cal.add(Calendar.DATE, 0);
		
		return String.format("%s 23:59:59", prevWaterMark);
	}

//	private String getDateString(Calendar cal) {
//		Date d = cal.getTime();
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");	
//		return String.format("%s 00:00:00", sdf.format(d));
//	}
//
	private long dateStr2Sec(String date) {
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d = null;
		try {
			d = sf.parse(date);
		} catch (ParseException e) {
			
			e.printStackTrace();
		}
		return d.getTime() / 1000;
	}

	private void updateWaterMark(String waterMark) {
		Writer writer = null;
		File file = new File(ctrlFile);

		try {
			writer = new FileWriter(file, false);
			String content = String.format("%s\t%s\t%s\n", shakeStatisURL, dataFrom, waterMark);
			writer.write(content);
			writer.close();
		} catch (IOException e) {
			LOGGER.error("Error writing water mark to control file!!!", e);
		}
	}
	
	private String getWaterMark(String defaultWaterMark) {
		File file = new File(ctrlFile);
		//String hostAndPort = String.format("%s:%s", hostName, port);

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
				if (statusInfo[0].equals(shakeStatisURL) && statusInfo[1].equals(dataFrom)) {
					reader.close();
					LOGGER.info(ctrlFile + " correctly formed");
					return statusInfo[2].replaceAll("\\r\\n|\\r|\\n", "");
				}
				else{
					LOGGER.warn(ctrlFile + " corrupt!!! Deleting it.");
					reader.close();
					//deleteControlFile();
					return defaultWaterMark;
				}
			} catch (IOException e) {
				LOGGER.error("Corrupt watermark value in file!!! Deleting it.", e);
				//deleteControlFile();
				return defaultWaterMark;
			}
		}
	}
	
	public static void main(String[] args) {
		Context ctx = new Context();
		ctx.put("data.from", "shake");
		ctx.put("shake.statis.url", "https://api.weixin.qq.com/shakearound/statistics/page?access_token=");
		ctx.put("shake.token.url", "http://weix.rtmap.com/mp/wxb5e69065eb3d67ce/token");
		ctx.put("sql.driver", "");
		ctx.put("sql.jdbc.url", "");
		ctx.put("sql.user", "");
		ctx.put("sql.passwd", "");
		ctx.put("sql.pageid.query", "");
		ctx.put("shake.ctrl.file", "c:/agent/test.ctl");
		ctx.put("shake.extract.init.date", "2016-01-01");

		Extractor ex = new ShakeExtractor();
		ex.init(ctx);
		while (ex.prepare()) {
			ex.cleanup();
		}
	}
}
