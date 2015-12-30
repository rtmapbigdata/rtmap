package cn.rtmap.bigdata.ingest.intercept;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.impl.HeaderConstants;

public class EventSerializer extends AbstractFlumeInterceptor {
	static final Logger logger = LoggerFactory.getLogger(EventSerializer.class);
	final static String COLUMNS = String.format("%s %s %s %s %s %s %s %s", HeaderConstants.DEF_TIMESTAMP, HeaderConstants.DEF_FROM, HeaderConstants.DEF_UNIT_CODE, HeaderConstants.DEF_PROCESS_MONTH, HeaderConstants.DEF_PROCESS_DATE, HeaderConstants.DEF_FILENAME, HeaderConstants.DEF_BATCH_ID, HeaderConstants.DEF_COMPRESS);
	final static String BODY = "body";
	final static String HEADERS = "headers";
	final static String DELIMITER = " ";

	static Context ctx;
	String columns;

	@Override
	public void initialize() {
		if (ctx != null) {
			columns = ctx.getString("columns", COLUMNS);
		} else {
			columns = COLUMNS;
		}
	}

	@Override
	public Event intercept(Event e) {
		Map<String, String> headers = e.getHeaders();
		Map<String, String> map = new HashMap<String, String>();
		String[] toks = columns.split(DELIMITER);
		for (String key : toks) {
			String val = headers.get(key);
			if (val != null) {
				map.put(key, val);
			}
		}
		
		JSONObject json = new JSONObject();
		//JSONObject.quote("");
		json.put(HEADERS, map);
		json.put(BODY, e.getBody());
		byte[] bytes = json.toString().getBytes();
		e.setBody(bytes);
		logger.info("data size after serialization: " + bytes.length + " bytes");
		return e;
	}

	@Override
	public void close() {}

	public static class Builder implements Interceptor.Builder {
		@Override
		public void configure(Context context) {
			ctx = context;
		}

		@Override
		public Interceptor build() {
			return new EventSerializer();
		}
	}
}
