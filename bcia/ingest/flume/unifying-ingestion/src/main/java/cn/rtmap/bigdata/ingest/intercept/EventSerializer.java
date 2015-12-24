package cn.rtmap.bigdata.ingest.intercept;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.JSONObject;

import cn.rtmap.bigdata.ingest.impl.HeaderConstants;

public class EventSerializer extends AbstractFlumeInterceptor {

	final static String COLUMNS = String.format("%s %s %s %s %s %s", HeaderConstants.DEF_FROM, HeaderConstants.DEF_UNIT_CODE, HeaderConstants.DEF_PROCESS_DATE, HeaderConstants.DEF_FILENAME, HeaderConstants.DEF_BATCH_ID, HeaderConstants.DEF_COMPRESS);
	final static String BODY = "body";
	final static String HEADERS = "headers";
	final static String DELIMITER = " ";

	//String columns;
	//String body;

	@Override
	public void initialize() {
		//body = BODY;
		//columns = COLUMNS;
	}

	@Override
	public Event intercept(Event e) {
		Map<String, String> headers = e.getHeaders();
		Map<String, String> map = new HashMap<String, String>();
		String[] toks = COLUMNS.split(DELIMITER);
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
		e.setBody(json.toString().getBytes());
		return e;
	}

//	@Override
//	public List<Event> intercept(List<Event> events) {
//		for (Iterator<Event> it = events.iterator(); it.hasNext();) {
//			Event next = intercept(it.next());
//			if (next == null) {
//				it.remove();
//			}
//		}
//		return events;
//	}

	@Override
	public void close() {
		// NOPE
		
	}

	public static class Builder implements Interceptor.Builder {
		@Override
		public void configure(Context context) {
			// NOPE
		}

		@Override
		public Interceptor build() {
			return new EventSerializer();
		}
	}
}
