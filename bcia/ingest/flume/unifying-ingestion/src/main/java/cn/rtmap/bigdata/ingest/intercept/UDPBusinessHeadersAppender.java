package cn.rtmap.bigdata.ingest.intercept;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.impl.HeaderConstants;

public class UDPBusinessHeadersAppender extends AbstractFlumeInterceptor {
	static final Logger logger = LoggerFactory.getLogger(UDPBusinessHeadersAppender.class);
	static final String HEADER_DATA_TIME = "data_time";
	static final String HEADER_TIMESTAMP = "timestamp";
	static final String HEADER_FIELD_SEPARATOR = "#";
	static final String HEADER_VAL_FROM = "lbs_online";

	@Override
	public void initialize() {}

	@Override
	public Event intercept(Event event) {
		Map<String, String> map = event.getHeaders();
		if (map != null) {
			String tm = map.get(HEADER_DATA_TIME);
			if (tm != null) {
				//logger.info("data_time:" + tm);
				long stamp = Long.parseLong(tm);
				Date d = new Date(stamp);
				SimpleDateFormat  sf = new SimpleDateFormat("yyyyMMdd");

				String dateStr = sf.format(d);
				map.put(HeaderConstants.DEF_PROCESS_DATE, dateStr);

				String yyyymm = dateStr.substring(0, "yyyymm".length());
				map.put(HeaderConstants.DEF_PROCESS_MONTH, yyyymm);

				map.put(HEADER_TIMESTAMP, tm);
				map.remove(HEADER_DATA_TIME);
			}

			String unitCode = map.get(HeaderConstants.DEF_UNIT_CODE);
			if (unitCode != null) {
				String[] toks = unitCode.split(HEADER_FIELD_SEPARATOR);
				if (toks != null && toks.length > 1) {
					map.put(HeaderConstants.DEF_UNIT_CODE, toks[0]);
					map.put(HeaderConstants.DEF_SERVICE_ID, toks[1]);
				}
			}

//			String p1 = map.get(HeaderConstants.DEF_PROCESS_DATE);
//			String p2 = map.get(HeaderConstants.DEF_UNIT_CODE);
//			if (p1 != null && p2 != null) {
//				String fileName = String.format("a_%s_%s", p1, p2);
//				map.put(HeaderConstants.DEF_FILENAME, fileName);
//			}
			map.put(HeaderConstants.DEF_MODE, HeaderConstants.VAL_MODE_DATA);
			map.put(HeaderConstants.DEF_FROM, HEADER_VAL_FROM);
		}
		return event;
	}

	@Override
	public void close() {}

	public static class Builder implements Interceptor.Builder {
		@Override
		public void configure(Context context) {
			// NOPE
		}

		@Override
		public Interceptor build() {
			return new UDPBusinessHeadersAppender();
		}
	}
}
