package cn.rtmap.bigdata.ingest.impl;

import java.io.IOException;
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
	}

	@Override
	public boolean prepare() {
		try {
			token = tokmgr.getToken();
			return true;
		} catch (IOException e) {
			LOGGER.error("Get token failed", e);
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

			String yyyymmdd = getYesterdayStr().substring(0, 10).replace("-", "");
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
	public void cleanup() {}

	private String getYesterdayStr() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		
		return getDateString(cal);
	}
	
	private String getTodayStr() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, 0);
		
		return getDateString(cal);
	}

	private String getDateString(Calendar cal) {
		Date d = cal.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");	
		return String.format("%s 00:00:00", sdf.format(d));
	}

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
}
